package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.TupleDomainFilter.NullsFilter;
import com.facebook.presto.orc.TupleDomainFilter.PositionalFilter;
import com.facebook.presto.spi.Subfield;

import java.util.Map;

public interface HierarchicalFilter
{
    HierarchicalFilter getChild();

    // Positional IS [NOT] NULL filters to apply at the next level
    NullsFilter getNullsFilter();

    // Positional range filters to apply at the very bottom level
    PositionalFilter getPositionalFilter();

    // Top-level offsets
    int[] getTopLevelOffsets();

    // Number of valid entries in the top-level offsets array
    int getTopLevelOffsetCount();

    // Filters per-position; positions with no filters are populated with nulls
    long[] getElementFilters();

    // Flags indicating top-level positions with at least one subfield with failed filter
    boolean[] getFailed();

    // Flags indicating top-level positions missing elements to apply subfield filters to
    boolean[] getIndexOutOfBounds();

    long getRetainedSizeInBytes();

    static HierarchicalFilter createHierarchicalFilter(StreamDescriptor streamDescriptor, Map<Subfield, TupleDomainFilter> subfieldFilters, int level, HierarchicalFilter parent)
    {
        switch (streamDescriptor.getStreamType())
        {
            case LIST:
                return new ListFilter(streamDescriptor, subfieldFilters, level, parent);
            case MAP:
            case STRUCT:
                throw new UnsupportedOperationException();
            default:
                return null;
        }
    }
}
