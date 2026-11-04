//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LongMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

public class ShortMaterializer extends ShortMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new ShortMaterializer(dataReader, (short) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new ShortMaterializer(dataReader, numValues);
        }
    };

    public static short convertValue(int value) {
        return (short) value;
    }

    private final ValuesReader dataReader;
    // region plainReader
    /** {@link #dataReader} when the page is PLAIN-encoded, else null. */
    private final PlainIntValuesReader plainReader;
    // endregion plainReader

    private ShortMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, (short) 0, numValues);
    }

    private ShortMaterializer(ValuesReader dataReader, short nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
        // region plainReader
        this.plainReader = dataReader instanceof PlainIntValuesReader plain ? plain : null;
        // endregion plainReader
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        // region plainReader
        if (plainReader != null) {
            // region plainCopy
            final int[] scratch = plainReader.scratch();
            for (int ii = startIndex; ii < endIndex;) {
                final int length = Math.min(endIndex - ii, scratch.length);
                plainReader.readIntegers(scratch, 0, length);
                for (int jj = 0; jj < length; jj++) {
                    data[ii + jj] = convertValue(scratch[jj]);
                }
                ii += length;
            }
            // endregion plainCopy
            return;
        }
        // endregion plainReader
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readInteger());
        }
    }
}
