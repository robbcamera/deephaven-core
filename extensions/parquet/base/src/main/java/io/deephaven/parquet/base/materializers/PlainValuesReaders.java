//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * Deephaven's readers for PLAIN-encoded fixed-width pages. Each supports the full per-value {@link ValuesReader}
 * interface for its type, so any materializer can consume one; the materializers in this package also recognize them
 * and copy values out in bulk.
 */
public final class PlainValuesReaders {

    private PlainValuesReaders() {}

    /**
     * @param type the column's parquet primitive type
     * @param in the page buffer, positioned past the repetition and definition levels and limited to the end of the
     *        page
     * @return a reader for a PLAIN page of {@code type}, or null if there is none for that type
     */
    @Nullable
    public static ValuesReader make(final PrimitiveTypeName type, final ByteBuffer in) {
        return switch (type) {
            case INT32 -> new PlainIntValuesReader(in);
            case INT64 -> new PlainLongValuesReader(in);
            case FLOAT -> new PlainFloatValuesReader(in);
            case DOUBLE -> new PlainDoubleValuesReader(in);
            default -> null;
        };
    }
}
