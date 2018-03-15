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
package com.facebook.presto.geospatial;

import io.airlift.slice.*;
import javafx.scene.shape.Polyline;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.geospatial.GeometryUtils.*;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.util.Objects.requireNonNull;

public class JtsGeometryUtils
{
    /**
     * Shape type codes from ERSI's specification
     * https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
     */
    private enum EsriShapeType
    {
        POINT(1),
        POLYLINE(3),
        POLYGON(5),
        MULTI_POINT(8);

        final int code;

        EsriShapeType(int code)
        {
            this.code = code;
        }

        static EsriShapeType valueOf(int code)
        {
            for (EsriShapeType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }

            throw new IllegalArgumentException("Unsupported ESRI shape code: " + code);
        }
    }

    /**
     * Shape type defined by JTS.
     */
    private static final String JTS_POINT = "Point";
    private static final String JTS_POLYGON = "Polygon";
    private static final String JTS_LINESTRING = "LineString";
    private static final String JTS_MULTI_POINT = "MultiPoint";
    private static final String JTS_MULTI_POLYGON = "MultiPolygon";
    private static final String JTS_MULTI_LINESTRING = "MultiLineString";
    private static final String JTS_GEOMETRY_COLLECTION = "GeometryCollection";

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private JtsGeometryUtils() {}

    /**
     * Deserializes ESRI shape as described in
     * https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
     * into JTS Geometry object.
     */
    public static Geometry deserialize(Slice shape)
    {
        if (shape == null) {
            return null;
        }

        BasicSliceInput input = shape.getInput();
        input.skip(SIZE_OF_INT); // SRID

        if (input.available() == 0) {
            return GEOMETRY_FACTORY.createGeometryCollection();
        }

        // GeometryCollection: spatialReferenceId|len-of-shape1|bytes-of-shape1|len-of-shape2|bytes-of-shape2...
        List<Geometry> geometries = new ArrayList<>();
        while (input.available() > 0) {
            int length = input.readInt();
            geometries.add(readGeometry(input.readSlice(length).getInput()));
        }

        if (geometries.size() == 1) {
            return geometries.get(0);
        }

        Geometry res = GEOMETRY_FACTORY.createGeometryCollection(geometries.toArray(new Geometry[0]));
        return res;
    }

    /**
     * Serialize JTS {@link Geometry} shape into an ESRI shape
     */
    public static Slice serialize(Geometry geometry) {
        int spatialReferenceId = geometry.getSRID();
        SliceOutput sliceOutput = new DynamicSliceOutput(100);
        sliceOutput.appendInt(spatialReferenceId);


        switch (geometry.getGeometryType()) {
            case JTS_POINT:
                writePoint((Point) geometry, sliceOutput);
                break;
            case JTS_MULTI_POINT:
                writeMultiPoint((MultiPoint) geometry, sliceOutput);
                break;
            case JTS_LINESTRING:
            case JTS_MULTI_LINESTRING:
                writePolyline(geometry, sliceOutput);
                break;
            case JTS_POLYGON:
            case JTS_MULTI_POLYGON:
                writePolygon(geometry, sliceOutput);
                break;
            case JTS_GEOMETRY_COLLECTION:
                for (int i = 0; i < geometry.getNumGeometries(); i++) {
                    Slice slice = serialize(geometry.getGeometryN(i));
                    sliceOutput.appendBytes(slice.getBytes(4, slice.length() - 4));

                }
                break;
        }

        return sliceOutput.slice();
    }


    /**
     * Utilize JTS to simplifies a geometry and ensures that the result is a valid geometry having the same dimension
     * and number of components as the input, and with the components having the same topological relationship.
     */
    public static Geometry simplify(Slice shape, double distanceTolerance) {
        if (shape == null) {
            return null;
        }

        return TopologyPreservingSimplifier.simplify(JtsGeometryUtils.deserialize(shape), distanceTolerance);
    }

    private static Geometry readGeometry(SliceInput input)
    {
        requireNonNull(input, "input is null");
        int geometryType = input.readInt();
        switch (EsriShapeType.valueOf(geometryType)) {
            case POINT:
                return readPoint(input);
            case POLYLINE:
                return readPolyline(input);
            case POLYGON:
                return readPolygon(input);
            case MULTI_POINT:
                return readMultiPoint(input);
            default:
                throw new UnsupportedOperationException("Invalid geometry type: " + geometryType);
        }
    }

    private static Point readPoint(SliceInput input)
    {
        requireNonNull(input, "input is null");
        Coordinate coordinates = readCoordinate(input);
        if (isEsriNaN(coordinates.x) || isEsriNaN(coordinates.y)) {
            return GEOMETRY_FACTORY.createPoint();
        }
        return GEOMETRY_FACTORY.createPoint(coordinates);
    }

    private static void writePoint(Point point, SliceOutput sliceOutput) {
        requireNonNull(sliceOutput, "output is null");
        sliceOutput.writeInt(SIZE_OF_DOUBLE * 2 + SIZE_OF_INT);
        sliceOutput.writeInt(EsriShapeType.POINT.code);
        if (!point.isEmpty()) {
            writeCoordinate(point.getCoordinate(), sliceOutput);
        } else {
            writeCoordinate(new Coordinate(Double.NaN, Double.NaN), sliceOutput);
        }
    }

    private static Coordinate readCoordinate(SliceInput input)
    {
        requireNonNull(input, "input is null");
        return new Coordinate(input.readDouble(), input.readDouble());
    }

    private static void writeCoordinate(Coordinate coordina, SliceOutput output) {
        requireNonNull(output, "output is null");
        output.writeDouble(coordina.x);
        output.writeDouble(coordina.y);
    }

    private static void writeCoordinates(Coordinate[] coordinates, SliceOutput output) {
        requireNonNull(coordinates, "coordinates is null");
        requireNonNull(output, "output is null");
        for (Coordinate coordinate : coordinates) {
            writeCoordinate(coordinate, output);
        }
    }

    private static Geometry readMultiPoint(SliceInput input)
    {
        requireNonNull(input, "input is null");
        skipEnvelope(input);
        int pointCount = input.readInt();
        Point[] points = new Point[pointCount];
        for (int i = 0; i < pointCount; i++) {
            points[i] = readPoint(input);
        }
        return GEOMETRY_FACTORY.createMultiPoint(points);
    }

    private static void writeMultiPoint(MultiPoint geometry, SliceOutput output) {
        requireNonNull(geometry, "geometry is null");
        requireNonNull(output, "output is null");

        int size = SIZE_OF_INT + SIZE_OF_DOUBLE * 4 + SIZE_OF_INT + SIZE_OF_DOUBLE * 2 * geometry.getNumPoints();
        output.writeInt(size);

        output.writeInt(EsriShapeType.MULTI_POINT.code);

        Envelope env = geometry.getEnvelopeInternal();
        writeEnvelope(env, output);

        output.writeInt(geometry.getNumPoints());
        Coordinate[] points = geometry.getCoordinates();
        for (Coordinate point : points) {
            writeCoordinate(point, output);
        }
    }

    private static Geometry readPolyline(SliceInput input)
    {
        requireNonNull(input, "input is null");
        skipEnvelope(input);
        int partCount = input.readInt();
        if (partCount == 0) {
            return GEOMETRY_FACTORY.createLineString();
        }

        int pointCount = input.readInt();
        LineString[] lineStrings = new LineString[partCount];

        int[] partLengths = new int[partCount];
        input.readInt();    // skip first index; it is always 0
        if (partCount > 1) {
            partLengths[0] = input.readInt();
            for (int i = 1; i < partCount - 1; i++) {
                partLengths[i] = input.readInt() - partLengths[i - 1];
            }
            partLengths[partCount - 1] = pointCount - partLengths[partCount - 2];
        }
        else {
            partLengths[0] = pointCount;
        }

        for (int i = 0; i < partCount; i++) {
            lineStrings[i] = GEOMETRY_FACTORY.createLineString(readCoordinates(input, partLengths[i]));
        }

        if (lineStrings.length == 1) {
            return lineStrings[0];
        }
        return GEOMETRY_FACTORY.createMultiLineString(lineStrings);
    }

    /**
     * serialize a geometry to a ESRI polyline.
     * Note: both jts multilinestring and linestring corresponds to polyline.
     *
     * @param lineStrings A {@link LineString} or {@link MultiLineString}
     */
    private static void writePolyline(Geometry lineStrings,
                                      SliceOutput output)
    {
        requireNonNull(lineStrings, "polyline is null");
        requireNonNull(output, "output is null");

        int partLengh = 0;
        if (lineStrings.getGeometryType() == JTS_MULTI_LINESTRING) {
            partLengh = lineStrings.getNumGeometries();
        }
        else if (lineStrings.getGeometryType() == JTS_LINESTRING) {
            partLengh = lineStrings.getNumPoints() > 0 ? 1 : 0;
        } else {
            throw new IllegalArgumentException("writePolyline can only handle multi-linestring or linestring");
        }

        // size
        int numPoints = lineStrings.getNumPoints();
        int size = SIZE_OF_INT + SIZE_OF_DOUBLE * 4 + SIZE_OF_INT * 2 + SIZE_OF_INT * partLengh +
                SIZE_OF_DOUBLE * 2 * numPoints;
        output.writeInt(size);

        // shape type
        output.writeInt(EsriShapeType.POLYLINE.code);

        // Box
        writeEnvelope(lineStrings.getEnvelopeInternal(), output);

        int[] lineStartPointIndex = new int[partLengh + 1];
        lineStartPointIndex[0] = 0;
        for (int i = 0; i < partLengh; i++) {
            int pointsInLine = lineStrings.getGeometryN(i).getNumPoints();
            lineStartPointIndex[i + 1] = pointsInLine;
        }

        // NumParts
        output.writeInt(partLengh);

        // NumPoints
        output.writeInt(numPoints);

        // Parts
        for (int i = 0; i < partLengh; i++) {
            output.writeInt(lineStartPointIndex[i]);
        }

        // Points
        writeCoordinates(lineStrings.getCoordinates(), output);
    }

    private static Coordinate[] readCoordinates(SliceInput input, int count)
    {
        requireNonNull(input, "input is null");
        verify(count > 0);
        Coordinate[] coordinates = new Coordinate[count];
        for (int j = 0; j < count; j++) {
            coordinates[j] = readCoordinate(input);
        }
        return coordinates;
    }

    private static Geometry readPolygon(SliceInput input)
    {
        requireNonNull(input, "input is null");
        skipEnvelope(input);
        int partCount = input.readInt();
        if (partCount == 0) {
            return GEOMETRY_FACTORY.createPolygon();
        }

        int pointCount = input.readInt();
        int[] startIndexes = new int[partCount];
        for (int i = 0; i < partCount; i++) {
            startIndexes[i] = input.readInt();
        }

        int[] partLengths = new int[partCount];
        if (partCount > 1) {
            partLengths[0] = startIndexes[1];
            for (int i = 1; i < partCount - 1; i++) {
                partLengths[i] = startIndexes[i + 1] - startIndexes[i];
            }
        }
        partLengths[partCount - 1] = pointCount - startIndexes[partCount - 1];

        LinearRing shell = null;
        List<LinearRing> holes = new ArrayList<>();
        List<Polygon> polygons = new ArrayList<>();
        for (int i = 0; i < partCount; i++) {
            Coordinate[] coordinates = readCoordinates(input, partLengths[i]);
            if (!isClockwise(coordinates)) {
                holes.add(GEOMETRY_FACTORY.createLinearRing(coordinates));
                continue;
            }
            if (shell != null) {
                polygons.add(GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0])));
            }
            shell = GEOMETRY_FACTORY.createLinearRing(coordinates);
            holes.clear();
        }
        polygons.add(GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0])));

        if (polygons.size() == 1) {
            return polygons.get(0);
        }
        return GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    private static void writePolygon(Geometry polygons, SliceOutput output) {
        requireNonNull(polygons, "polygon is null");
        requireNonNull(output, "output is null");
        if (polygons.getGeometryType() != JTS_POLYGON && polygons.getGeometryType() != JTS_MULTI_POLYGON) {
            throw new IllegalArgumentException("writePolyline can only handle multi-polygon or polygon");
        }

        int numGeometries = polygons.getNumGeometries();
        int numParts = 0;
        int numPoints = polygons.getNumPoints();
        for (int i = 0; i < numGeometries; i++) {
            Polygon polygon = (Polygon) polygons.getGeometryN(i);
            if (polygon.getNumPoints() > 0) {
                numParts += polygon.getNumInteriorRing() + 1;
            }
        }

        int size = SIZE_OF_INT + SIZE_OF_DOUBLE * 4 + SIZE_OF_INT * (2 + numParts)
                + SIZE_OF_DOUBLE * 2 * numPoints;
        output.writeInt(size);

        // shape type
        output.writeInt(EsriShapeType.POLYGON.code);

        // Box
        writeEnvelope(polygons.getEnvelopeInternal(), output);

        // Number of parts
        output.writeInt(numParts);

        // Number of points
        output.writeInt(polygons.getNumPoints());

        // Index of first point in part
        int startIndex[] = new int[numParts + 1];
        startIndex[0] = 0;
        int partIndex = 0;
        for (int i = 0; i < numGeometries; i++) {
            Polygon polygon = (Polygon) polygons.getGeometryN(i);
            if (polygon.getNumPoints() <= 0) {
                continue;
            }
            startIndex[++partIndex] = startIndex[partIndex - 1] + polygon.getExteriorRing().getNumPoints();
            for (int j = 0; j < polygon.getNumInteriorRing(); j++) {
                if (partIndex < numParts - 1) {
                    startIndex[++partIndex] = startIndex[partIndex - 1] + polygon.getInteriorRingN(j).getNumPoints();
                }
            }
        }

        for (int i = 0; i < numParts; i++) {
            // ignore the last element of startIndex.
            output.writeInt(startIndex[i]);
        }

        // Points
        writeCoordinates(polygons.getCoordinates(), output);
    }


    private static void skipEnvelope(SliceInput input)
    {
        requireNonNull(input, "input is null");
        input.skip(4 * SIZE_OF_DOUBLE);
    }


    private static void writeEnvelope(Envelope envelope, SliceOutput sliceOutput)
    {
        requireNonNull(envelope, "envelope is null");
        requireNonNull(sliceOutput, "output is null");

        sliceOutput.writeDouble(envelope.getMinX());
        sliceOutput.writeDouble(envelope.getMinY());
        sliceOutput.writeDouble(envelope.getMaxX());
        sliceOutput.writeDouble(envelope.getMaxY());
    }

    private static boolean isClockwise(Coordinate[] coordinates)
    {
        // Sum over the edges: (x2 − x1) * (y2 + y1).
        // If the result is positive the curve is clockwise,
        // if it's negative the curve is counter-clockwise.
        double area = 0;
        for (int i = 1; i < coordinates.length; i++) {
            area += (coordinates[i].x - coordinates[i - 1].x) * (coordinates[i].y + coordinates[i - 1].y);
        }
        area += (coordinates[0].x - coordinates[coordinates.length - 1].x) * (coordinates[0].y + coordinates[coordinates.length - 1].y);
        return area > 0;
    }
}
