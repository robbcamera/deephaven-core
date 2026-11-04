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

public class DoubleFromFloatMaterializer extends DoubleMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new DoubleFromFloatMaterializer(dataReader, (double) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new DoubleFromFloatMaterializer(dataReader, numValues);
        }
    };

    public static double convertValue(float value) {
        return value;
    }

    private final ValuesReader dataReader;
    // region plainReader
    /** {@link #dataReader} when the page is PLAIN-encoded, else null. */
    private final PlainFloatValuesReader plainReader;
    // endregion plainReader

    private DoubleFromFloatMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private DoubleFromFloatMaterializer(ValuesReader dataReader, double nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
        // region plainReader
        this.plainReader = dataReader instanceof PlainFloatValuesReader plain ? plain : null;
        // endregion plainReader
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        // region plainReader
        if (plainReader != null) {
            // region plainCopy
            final float[] scratch = plainReader.scratch();
            for (int ii = startIndex; ii < endIndex;) {
                final int length = Math.min(endIndex - ii, scratch.length);
                plainReader.readFloats(scratch, 0, length);
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
            data[ii] = convertValue(dataReader.readFloat());
        }
    }
}
