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
#pragma once

#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::presto::operators {

class BroadcastFileWriter {
 public:
  BroadcastFileWriter(
      std::unique_ptr<velox::WriteFile> writeFile,
      std::string_view filename,
      velox::memory::MemoryPool* pool,
      const velox::RowTypePtr& inputType);

  virtual ~BroadcastFileWriter() = default;

  /// Write to file
  void collect(velox::RowVectorPtr input);

  /// Flush the data
  void noMoreData();

  /// File stats - path, size, checksum, num rows.
  velox::RowVectorPtr fileStats();

 private:
  void serialize(const velox::RowVectorPtr& rowVector);

  std::unique_ptr<velox::WriteFile> writeFile_;
  std::string filename_;
  velox::memory::MemoryPool* pool_;
  std::unique_ptr<velox::VectorSerde> serde_;
  const velox::RowTypePtr& inputType_;
};

class FileBroadcast {
 public:
  FileBroadcast(const std::string& basePath);

  virtual ~FileBroadcast() = default;

  std::unique_ptr<BroadcastFileWriter> createWriter(
      velox::memory::MemoryPool* pool,
      const velox::RowTypePtr& inputType); // add stage, task id

 private:
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;
  const std::string& basePath_;
};
} // namespace facebook::presto::operators
