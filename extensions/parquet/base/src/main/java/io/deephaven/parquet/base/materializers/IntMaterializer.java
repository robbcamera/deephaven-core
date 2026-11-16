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

public class IntMaterializer extends IntMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new IntMaterializer(dataReader, (int) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new IntMaterializer(dataReader, numValues);
        }
    };

    public static int convertValue(int value) {
        return value;
    }

    private final ValuesReader dataReader;
    // region plainReader
    /** {@link #dataReader} when the page is PLAIN-encoded, else null. */
    private final PlainIntValuesReader plainReader;
    // endregion plainReader

    private IntMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private IntMaterializer(ValuesReader dataReader, int nullValue, int numValues) {
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
            plainReader.readIntegers(data, startIndex, endIndex - startIndex);
            // endregion plainCopy
            return;
        }
        // endregion plainReader
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readInteger());
        }
    }
}
