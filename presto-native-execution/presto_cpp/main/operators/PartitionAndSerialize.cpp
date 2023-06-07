/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include <folly/lang/Bits.h>
#include "velox/row/UnsafeRowFast.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {
velox::core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

/// The output of this operator has 2 columns:
/// (1) partition number (INTEGER);
/// (2) serialized row (VARBINARY)
class PartitionAndSerializeOperator : public Operator {
 public:
  PartitionAndSerializeOperator(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL ctx,
      const std::shared_ptr<const PartitionAndSerializeNode>& planNode)
      : Operator(
            ctx,
            planNode->outputType(),
            operatorId,
            planNode->id(),
            "PartitionAndSerialize"),
        numPartitions_(planNode->numPartitions()),
        replicateNullsAndAny_(
            numPartitions_ > 1 ? planNode->isReplicateNullsAndAny() : false),
        partitionFunction_(
            numPartitions_ == 1 ? nullptr
                                : planNode->partitionFunctionFactory()->create(
                                      planNode->numPartitions())),
        serializedRowType_{planNode->serializedRowType()},
        keyChannels_(toChannels(
            planNode->sources()[0]->outputType(),
            planNode->keys())) {
    const auto& inputType = planNode->sources()[0]->outputType()->asRow();
    const auto& serializedRowTypeNames = serializedRowType_->names();
    bool identityMapping = true;
    for (auto i = 0; i < serializedRowTypeNames.size(); ++i) {
      serializedColumnIndices_.push_back(
          inputType.getChildIdx(serializedRowTypeNames[i]));
      if (serializedColumnIndices_.back() != i) {
        identityMapping = false;
      }
    }
    if (identityMapping) {
      serializedColumnIndices_.clear();
    }
  }

  bool needsInput() const override {
    return !input_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
    if (replicateNullsAndAny_) {
      collectNullRows();
    }
    processingFirstBatch_ = true;
    nextPartitionId_ = 0;
  }

  /// Based on the characteristic of input vector we will process it differently
  /// in the function. When 'replicateNullsAndAny_' is true and input vector is
  /// null skewed, the output vector size could be significantly larger than the
  /// input size. To handle such large output vector, we will yield output in
  /// batch. There are total of 3 branch of execution:
  /// Branch 1) 'replicateNullsAndAny_' is false, partition and serialize input
  /// elements only.
  ///
  /// Branch 2) 'replicateNullsAndAny_' is true and input vector
  /// is not null skewed, partition and serialize input elements along with
  /// replicate null/any row(s).
  ///
  /// Branch 3) 'replicateNullsAndAny_' is true and
  /// input vector is null skewed, Do this in steps:
  ///
  /// Step 1) Same as Branch 1.
  ///
  /// Step 2) Replicate null values for 'nextPartitionId_' and repeat with
  /// nextPartitionId_ + 1
  RowVectorPtr getOutput() override {
    if (!input_) {
      return nullptr;
    }

    vector_size_t numInput = input_->size();
    velox::row::UnsafeRowFast unsafeRow(reorderInputsIfNeeded());
    calculateSerializedRowSize(unsafeRow);

    std::optional<uint64_t> averageRowSize;
    if (!cumulativeRowSizes_.empty()) {
      averageRowSize = cumulativeRowSizes_[cumulativeRowSizes_.size() - 1] /
          cumulativeRowSizes_.size();
    }
    const vector_size_t maxOutputSize = outputBatchRows(averageRowSize);
    auto output = createOutputRowVector(maxOutputSize);
    auto partitionVector = output->childAt(0)->asFlatVector<int32_t>();
    auto dataVector = output->childAt(1)->asFlatVector<StringView>();

    if (processingFirstBatch_) {
      computePartitions(*partitionVector);
      serializeRows(unsafeRow, *dataVector);
      processingFirstBatch_ = false;
      // We can fit remaining elements into the output vector in the same pass.
      if (replicateNullsAndAny_ && maxOutputSize > numInput) {
        auto numOutputRows = replicateRowsWithNullPartitionKeys(
            *partitionVector, *dataVector, 0, numPartitions_, numInput);
        // If numOutputRows equal to the input row count, then
        // 'replicateRowsWithNullPartitionKeys' did not insert any row into
        // the output vector, thus we need to call into
        // 'replicateAnyToEmptyPartitions'
        if (numOutputRows == input_->size()) {
          numOutputRows = replicateAnyToEmptyPartitions(
              *partitionVector, *dataVector, numOutputRows);
        }
        resizeOutputVector(output, numOutputRows);
      }
      // Clear input row to indicate all processing has finished.
      input_ = nullptr;
    } else {
      VELOX_CHECK(replicateNullsAndAny_);

      vector_size_t outputSize = 0;
      do {
        outputSize += replicateRowsWithNullPartitionKeys(
            *partitionVector,
            *dataVector,
            nextPartitionId_,
            nextPartitionId_ + 1,
            outputSize);
        ++nextPartitionId_;
        // Keep processing until we have filled up the output vector OR we are
        // done with all partitions.
      } while (outputSize + numInput <= maxOutputSize &&
               nextPartitionId_ < numPartitions_);

      resizeOutputVector(output, outputSize);

      // We have processed all partitions, there are no more output vector(s) to
      // yield
      if (nextPartitionId_ == numPartitions_) {
        input_ = nullptr;
      }
    }
    return output;
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

  /// The size of the output vector depends on the value of
  /// 'replicateNullsAndAny_'. Since we are processing the output
  /// vector in stages, the size of the output vector will depend on the stage
  /// of the operator 'processingFirstBatch_'. It is ok to over-allocate and
  /// then resize to a lower value instead of paying reallocation(s) cost.
  uint32_t outputBatchRows(std::optional<uint64_t> averageRowSize) const {
    auto desiredOutputBatchRows = Operator::outputBatchRows(averageRowSize);
    const vector_size_t numInput = input_->size();
    vector_size_t maxOutputSize = numInput;
    if (replicateNullsAndAny_) {
      // If we employ replicateNull strategy, we won't need to apply
      // replicateAny, hence we use max to select one or the other
      const int32_t additionalElements = std::max(
          nullRows_.countSelected() * (numPartitions_ - 1), numPartitions_);
      if (maxOutputSize + additionalElements <= desiredOutputBatchRows) {
        maxOutputSize += additionalElements;
      } else {
        if (!processingFirstBatch_) {
          // When processing null values, we want the output vector to be
          // at-least 'desiredOutputBatchRows' long.
          int32_t nextDesiredSize = std::pow(
              2, static_cast<int32_t>(std::log2(desiredOutputBatchRows)) + 1);
          maxOutputSize = nextDesiredSize;
        }
      }
    }
    return maxOutputSize;
  }

 private:
  // Create output vector and resize all it's children appropriately.
  RowVectorPtr createOutputRowVector(vector_size_t size) {
    auto output = BaseVector::create<RowVector>(outputType_, size, pool());
    for (auto& child : output->children()) {
      child->resize(size);
    }

    if (serializedDataBuffer_ != nullptr) {
      auto dataVector = output->childAt(1)->asFlatVector<StringView>();
      // Hold the reference to serialized string buffer.
      dataVector->addStringBuffer(serializedDataBuffer_);
    }
    return output;
  }

  // Resize output vector and all of it's children.
  void resizeOutputVector(const RowVectorPtr& output, vector_size_t size) {
    output->resize(size);
    for (auto& child : output->children()) {
      child->resize(size);
    }
  }

  // Replicate input rows with null partition keys to partitions
  // [startPartitionId, endPartitionId). Return the total number of output rows
  // after the replication.
  vector_size_t replicateRowsWithNullPartitionKeys(
      FlatVector<int32_t>& partitionVector,
      FlatVector<StringView>& dataVector,
      vector_size_t startPartitionId,
      vector_size_t endPartitionId,
      vector_size_t nextRow) {
    auto rawBuffer =
        serializedDataBuffer_->as<char>() + serializedDataVectorOffset_;

    for (auto partition = startPartitionId; partition < endPartitionId;
         ++partition) {
      nullRows_.applyToSelected([&](auto row) {
        // Replicate a row with null partition key to all the other partitions.
        if (partitions_[row] != partition) {
          partitionVector.set(nextRow, partition);
          dataVector.setNoCopy(
              nextRow,
              StringView(rawBuffer + cumulativeRowSizes_[row], rowSizes_[row]));
          ++nextRow;
        }
      });
    }
    return nextRow;
  }

  // Replicates the first value to all the empty partitions.
  vector_size_t replicateAnyToEmptyPartitions(
      FlatVector<int32_t>& partitionsVector,
      FlatVector<StringView>& dataVector,
      vector_size_t nextRow) {
    auto* rawPartitions = partitionsVector.mutableRawValues();
    // Since we can replicate any row, we prefer to replicate the smallest.
    vector_size_t minValueIndex = std::distance(
        rowSizes_.begin(),
        std::min_element(rowSizes_.begin(), rowSizes_.end()));
    StringView anyValue = dataVector.valueAt(minValueIndex);

    folly::F14FastSet<uint32_t> nonEmptyPartitions(
        rawPartitions, rawPartitions + numPartitions_);

    vector_size_t index = nextRow;
    for (auto i = 0; i < numPartitions_; ++i) {
      if (!nonEmptyPartitions.contains(i)) {
        rawPartitions[index] = i;
        dataVector.setNoCopy(index, anyValue);
        ++index;
      }
    }
    return index;
  }

  // A partition key is considered null if any of the partition key column in
  // 'input_' is null. The function records the rows has null partition key in
  // 'nullRows_'.
  void collectNullRows() {
    const auto numInput = input_->size();
    nullRows_.resize(numInput);
    nullRows_.clearAll();

    // TODO Avoid decoding keys twice: here and when computing partitions.
    decodedVectors_.resize(keyChannels_.size());

    for (auto partitionKey : keyChannels_) {
      auto& keyVector = input_->childAt(partitionKey);
      if (keyVector->mayHaveNulls()) {
        auto& decoded = decodedVectors_[partitionKey];
        decoded.decode(*keyVector);
        if (auto* rawNulls = decoded.nulls()) {
          bits::orWithNegatedBits(
              nullRows_.asMutableRange().bits(), rawNulls, 0, numInput);
        }
      }
    }
    nullRows_.updateBounds();
  }

  void computePartitions(FlatVector<int32_t>& partitionsVector) {
    auto numInput = input_->size();
    partitions_.resize(numInput);
    if (numPartitions_ == 1) {
      std::fill(partitions_.begin(), partitions_.end(), 0);
    } else {
      partitionFunction_->partition(*input_, partitions_);
    }

    // TODO Avoid copy.
    auto rawPartitions = partitionsVector.mutableRawValues();
    ::memcpy(rawPartitions, partitions_.data(), sizeof(int32_t) * numInput);
  }

  RowVectorPtr reorderInputsIfNeeded() {
    if (serializedColumnIndices_.empty()) {
      return input_;
    }

    const auto& inputColumns = input_->children();
    std::vector<VectorPtr> columns(inputColumns.size());
    for (auto i = 0; i < columns.size(); ++i) {
      columns[i] = inputColumns[serializedColumnIndices_[i]];
    }
    return std::make_shared<RowVector>(
        input_->pool(),
        serializedRowType_,
        nullptr,
        input_->size(),
        std::move(columns));
  }

  // Calculate the size of each serialized row
  void calculateSerializedRowSize(velox::row::UnsafeRowFast& unsafeRow) {
    const auto numInput = input_->size();

    // Compute row sizes.
    rowSizes_.resize(numInput);
    cumulativeRowSizes_.resize(numInput);

    size_t totalSize = 0;
    if (auto fixedRowSize = unsafeRow.fixedRowSize(asRowType(input_->type()))) {
      totalSize += fixedRowSize.value() * numInput;
      std::fill(rowSizes_.begin(), rowSizes_.end(), fixedRowSize.value());
      // Calculate the cumulative sum starting from index 0 and finish at n-1
      // cumulativeRowSizes_ should start with value 0 at index 0.
      std::partial_sum(
          rowSizes_.begin(),
          rowSizes_.end() - 1,
          cumulativeRowSizes_.begin() + 1);
    } else {
      for (auto i = 0; i < numInput; ++i) {
        const size_t rowSize = unsafeRow.rowSize(i);
        rowSizes_[i] = rowSize;
        cumulativeRowSizes_[i] = totalSize;
        totalSize += rowSize;
      }
    }
  }

  // The logic of this method is logically identical with
  // UnsafeRowVectorSerializer::append() and UnsafeRowVectorSerializer::flush().
  // Rewriting of the serialization logic here to avoid additional copies so
  // that contents are directly written into passed in vector.
  void serializeRows(
      velox::row::UnsafeRowFast& unsafeRow,
      FlatVector<StringView>& dataVector) {
    const auto numInput = input_->size();
    size_t totalSize = cumulativeRowSizes_[cumulativeRowSizes_.size() - 1] +
        rowSizes_[rowSizes_.size() - 1];

    // Allocate memory.
    serializedDataBuffer_ = dataVector.getBufferWithSpace(totalSize);
    // getBufferWithSpace() may return a buffer that already has content, so we
    // only use the space after that.
    serializedDataVectorOffset_ = serializedDataBuffer_->size();
    auto rawBuffer =
        serializedDataBuffer_->asMutable<char>() + serializedDataVectorOffset_;
    serializedDataBuffer_->setSize(serializedDataBuffer_->size() + totalSize);
    memset(rawBuffer, 0, totalSize);

    // Serialize rows.
    size_t offset = 0;
    for (auto i = 0; i < numInput; ++i) {
      dataVector.setNoCopy(i, StringView(rawBuffer + offset, rowSizes_[i]));

      // Write row data.
      auto size = unsafeRow.serialize(i, rawBuffer + offset);
      VELOX_DCHECK_EQ(size, rowSizes_[i]);
      offset += size;
    }
  }

  const uint32_t numPartitions_;
  const RowTypePtr serializedRowType_;
  const std::vector<column_index_t> keyChannels_;
  const std::unique_ptr<core::PartitionFunction> partitionFunction_;
  const bool replicateNullsAndAny_;
  std::vector<column_index_t> serializedColumnIndices_;

  bool processingFirstBatch_{true};
  vector_size_t nextPartitionId_{0};

  // Decoded 'keyChannels_' columns.
  std::vector<velox::DecodedVector> decodedVectors_;
  // Identifies the input rows which has null partition keys.
  velox::SelectivityVector nullRows_;
  // Reusable vector for storing partition id for each input row.
  std::vector<uint32_t> partitions_;
  // Reusable vector for storing serialized row size for each input row.
  std::vector<uint32_t> rowSizes_;

  // For processing null skewed input vector.
  // Keep reference of serialized data vector, this is used when processing
  // subsequent batches.
  BufferPtr serializedDataBuffer_;
  // Offset storing the starting index of serialized data in 'rawBuffer'.
  size_t serializedDataVectorOffset_;
  // Reusable vector for storing cumulative row size for each input row.
  std::vector<uint32_t> cumulativeRowSizes_;
};
} // namespace

std::unique_ptr<Operator> PartitionAndSerializeTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto partitionNode =
          std::dynamic_pointer_cast<const PartitionAndSerializeNode>(node)) {
    return std::make_unique<PartitionAndSerializeOperator>(
        id, ctx, partitionNode);
  }
  return nullptr;
}

void PartitionAndSerializeNode::addDetails(std::stringstream& stream) const {
  stream << "(";
  for (auto i = 0; i < keys_.size(); ++i) {
    const auto& expr = keys_[i];
    if (i > 0) {
      stream << ", ";
    }
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
      stream << field->name();
    } else if (
        auto constant =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
      stream << constant->toString();
    } else {
      stream << expr->toString();
    }
  }
  stream << ") " << numPartitions_ << " " << partitionFunctionSpec_->toString()
         << " " << serializedRowType_->toString();
}

folly::dynamic PartitionAndSerializeNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["keys"] = ISerializable::serialize(keys_);
  obj["numPartitions"] = numPartitions_;
  obj["serializedRowType"] = serializedRowType_->serialize();
  obj["sources"] = ISerializable::serialize(sources_);
  obj["replicateNullsAndAny"] = replicateNullsAndAny_;
  obj["partitionFunctionSpec"] = partitionFunctionSpec_->serialize();
  return obj;
}

velox::core::PlanNodePtr PartitionAndSerializeNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<PartitionAndSerializeNode>(
      deserializePlanNodeId(obj),
      ISerializable::deserialize<std::vector<velox::core::ITypedExpr>>(
          obj["keys"], context),
      obj["numPartitions"].asInt(),
      ISerializable::deserialize<RowType>(obj["serializedRowType"], context),
      ISerializable::deserialize<std::vector<velox::core::PlanNode>>(
          obj["sources"], context)[0],
      obj["replicateNullsAndAny"].asBool(),
      ISerializable::deserialize<velox::core::PartitionFunctionSpec>(
          obj["partitionFunctionSpec"], context));
}
} // namespace facebook::presto::operators
