//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit PlainLongValuesReader and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

/**
 * Reads PLAIN-encoded int values from the page buffer.
 * <p>
 * Materializers copy runs of values out with {@link #readIntegers} and convert them in a loop with no calls. A per-value
 * read, whether {@link #readInteger()} or a {@code VarHandle}, is fast only when C2 inlines it into the decode loop, and
 * the loop sits deep enough in the page-read call chain that it often doesn't.
 */
class PlainIntValuesReader extends ValuesReader {

    /** The most values {@link #scratch()} holds. */
    static final int SCRATCH_SIZE = 1024;

    private final IntBuffer values;
    /** Index into {@link #values} of the next value. */
    private int next;
    private int[] scratch;

    /**
     * @param in A buffer positioned at the first value and limited to the end of the page
     */
    PlainIntValuesReader(final ByteBuffer in) {
        values = in.slice().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    }

    /**
     * Consume {@code count} values, bounds-checked against the page. The check is not optional: the page buffer is
     * reused and over-allocated, so reading past the page would quietly return stale bytes from the previous one.
     *
     * @return the index into {@link #values} of the first value consumed
     */
    private int claim(final int count) {
        final int start = next;
        if (count > values.limit() - start) {
            throw new ParquetDecodingException("Ran out of page data reading " + count + " PLAIN int values; "
                    + (values.limit() - start) + " remain");
        }
        next = start + count;
        return start;
    }

    void readIntegers(final int[] dest, final int offset, final int length) {
        values.get(claim(length), dest, offset, length);
    }

    /**
     * @return a buffer for converting materializers to copy runs into with {@link #readIntegers}. Never empty, so a copy
     *         loop always advances and over-reading fails in {@link #claim} rather than spinning.
     */
    int[] scratch() {
        if (scratch == null) {
            scratch = new int[Math.max(1, Math.min(values.limit(), SCRATCH_SIZE))];
        }
        return scratch;
    }

    @Override
    public int readInteger() {
        return values.get(claim(1));
    }

    @Override
    public void skip() {
        claim(1);
    }

    @Override
    public void skip(final int n) {
        claim(n);
    }
}
