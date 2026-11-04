//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

public class ShortFromUnsignedByteMaterializer extends ShortMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new ShortFromUnsignedByteMaterializer(dataReader, (short) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new ShortFromUnsignedByteMaterializer(dataReader, numValues);
        }
    };

    public static short convertValue(int value) {
        return (short) Byte.toUnsignedInt((byte) value);
    }

    private final ValuesReader dataReader;
    /** {@link #dataReader} when the page is PLAIN-encoded, else null. */
    private final PlainIntValuesReader plainReader;

    private ShortFromUnsignedByteMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, (short) 0, numValues);
    }

    private ShortFromUnsignedByteMaterializer(ValuesReader dataReader, short nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
        this.plainReader = dataReader instanceof PlainIntValuesReader plain ? plain : null;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        if (plainReader != null) {
            final int[] scratch = plainReader.scratch();
            for (int ii = startIndex; ii < endIndex;) {
                final int length = Math.min(endIndex - ii, scratch.length);
                plainReader.readIntegers(scratch, 0, length);
                for (int jj = 0; jj < length; jj++) {
                    data[ii + jj] = convertValue(scratch[jj]);
                }
                ii += length;
            }
            return;
        }
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readInteger());
        }
    }
}
