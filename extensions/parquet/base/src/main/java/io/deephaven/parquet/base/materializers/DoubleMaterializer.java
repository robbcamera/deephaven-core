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

public class DoubleMaterializer extends DoubleMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new DoubleMaterializer(dataReader, (double) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new DoubleMaterializer(dataReader, numValues);
        }
    };

    public static double convertValue(double value) {
        return value;
    }

    private final ValuesReader dataReader;
    // region plainReader
    /** {@link #dataReader} when the page is PLAIN-encoded, else null. */
    private final PlainDoubleValuesReader plainReader;
    // endregion plainReader

    private DoubleMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private DoubleMaterializer(ValuesReader dataReader, double nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
        // region plainReader
        this.plainReader = dataReader instanceof PlainDoubleValuesReader plain ? plain : null;
        // endregion plainReader
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        // region plainReader
        if (plainReader != null) {
            // region plainCopy
            plainReader.readDoubles(data, startIndex, endIndex - startIndex);
            // endregion plainCopy
            return;
        }
        // endregion plainReader
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readDouble());
        }
    }
}
