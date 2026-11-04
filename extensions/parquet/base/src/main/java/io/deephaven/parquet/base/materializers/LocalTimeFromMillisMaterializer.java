//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LocalTimeFromMicrosMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalTime;

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MILLI;

public class LocalTimeFromMillisMaterializer extends ObjectMaterializerBase<LocalTime> implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalTimeFromMillisMaterializer(dataReader, (LocalTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalTimeFromMillisMaterializer(dataReader, numValues);
        }
    };

    public static LocalTime convertValue(int value) {
        return LocalTime.ofNanoOfDay(value * MILLI);
    }

    private final ValuesReader dataReader;
    /** {@link #dataReader} when the page is PLAIN-encoded, else null. */
    private final PlainIntValuesReader plainReader;

    private LocalTimeFromMillisMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalTimeFromMillisMaterializer(ValuesReader dataReader, LocalTime nullValue, int numValues) {
        super(nullValue, new LocalTime[numValues]);
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
