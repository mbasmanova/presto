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

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.geospatial.GeometryUtils.isEsriNaN;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
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
    private enum JtsShapeType {
        POINT("Point"),
        POLYGON("Polygon"),
        MULTI_LINESTRING("MultiLineString"), // correspond to POLYLINE in ESRI shape type
        MULTI_POINT("MultiPoint");

        final String typeString;

        JtsShapeType(String type) {
            this.typeString = type;
        }

        @Override
        public String toString() {
            return typeString;
        }
    }

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
        int spatialReferenceId = input.readInt();

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
        res.setSRID(spatialReferenceId);
        return res;
    }

    /**
     * Serialize JTS {@link Geometry} shape into an ESRI shape
     */
    public static Slice serialize(Geometry geometry) {
        int spatialReferenceId = geometry.getSRID();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(100);
        sliceOutput.appendInt(spatialReferenceId);

        int availableGeometry = geometry.getNumGeometries();
        for (int i = 0; i < availableGeometry; i++) {
            Geometry unitGeometry = geometry.getGeometryN(i);
            switch (JtsShapeType.valueOf(unitGeometry.getGeometryType())) {
                case POINT:
                    writePoint((Point) unitGeometry, sliceOutput);
                    break;
                case MULTI_LINESTRING:
                    writePolylineOrPolygon((GeometryCollection) unitGeometry, sliceOutput, EsriShapeType.POLYLINE);
                    break;
                case POLYGON:
                    writePolylineOrPolygon((GeometryCollection) unitGeometry, sliceOutput, EsriShapeType.POLYGON);
                    break;
                case MULTI_POINT:
                    writeMultiPoint((MultiPoint) unitGeometry, sliceOutput);
                    break;
            }
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

        Geometry geometry = JtsGeometryUtils.deserialize(shape);
        return TopologyPreservingSimplifier.simplify(geometry, distanceTolerance);
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

    private static void writePoint(Point point, DynamicSliceOutput sliceOutput) {
        requireNonNull(sliceOutput, "output is null");
        if (!point.isEmpty()) {
            sliceOutput.writeInt(EsriShapeType.POINT.code);
            writeCoordinate(point.getCoordinate(), sliceOutput);
        } else {
            throw new IllegalArgumentException("Trying to write an empty point.");
        }
    }

    private static Coordinate readCoordinate(SliceInput input)
    {
        requireNonNull(input, "input is null");
        return new Coordinate(input.readDouble(), input.readDouble());
    }

    private static void writeCoordinate(Coordinate point, DynamicSliceOutput sliceOutput) {
        requireNonNull(sliceOutput, "output is null");
        sliceOutput.writeDouble(point.x);
        sliceOutput.writeDouble(point.y);
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

    private static void writeMultiPoint(MultiPoint geometry, DynamicSliceOutput sliceOutput) {
        requireNonNull(geometry, "geometry is null");
        requireNonNull(sliceOutput, "output is null");

        sliceOutput.writeInt(EsriShapeType.MULTI_POINT.code);
        Envelope env = geometry.getEnvelopeInternal();
        writeEnvelope(env, sliceOutput);

        sliceOutput.writeInt(geometry.getNumPoints());
        Coordinate[] points = geometry.getCoordinates();
        for (Coordinate point : points) {
            writeCoordinate(point, sliceOutput);
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


    private static void writePolylineOrPolygon(GeometryCollection geometries,
                                               DynamicSliceOutput sliceOutput,
                                               EsriShapeType esriShapeType)
    {
        requireNonNull(geometries, "polyline is null");
        requireNonNull(sliceOutput, "output is null");

        if (esriShapeType != EsriShapeType.POLYGON && esriShapeType != EsriShapeType.POLYLINE) {
            throw new IllegalArgumentException("writePolylineOrPolygon only takes polygon or polyline");
        }

        sliceOutput.writeInt(esriShapeType.code);

        writeEnvelope(geometries.getEnvelopeInternal(), sliceOutput);

        int partLengh = geometries.getNumGeometries();

        // calculate total number of points
        int totalPoints = 0;
        int[] lineStartPointIndex = new int[partLengh + 1];
        lineStartPointIndex[0] = 0;
        for (int i = 0; i < partLengh; i++) {
            int pointsInLine = geometries.getGeometryN(i).getNumPoints();
            totalPoints += pointsInLine;
            lineStartPointIndex[i + 1] = pointsInLine;
        }

        sliceOutput.writeInt(partLengh);
        sliceOutput.writeInt(totalPoints);
        for (int i = 0; i < lineStartPointIndex.length; i++) {
            sliceOutput.writeInt(lineStartPointIndex[i]);
        }
        Coordinate[] points = geometries.getCoordinates();
        for (int i = 0; i < points.length; i++) {
            writeCoordinate(points[i], sliceOutput);
        }
    }

    private static void skipEnvelope(SliceInput input)
    {
        requireNonNull(input, "input is null");
        input.skip(4 * SIZE_OF_DOUBLE);
    }


    private static void writeEnvelope(Envelope envelope, DynamicSliceOutput sliceOutput)
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
