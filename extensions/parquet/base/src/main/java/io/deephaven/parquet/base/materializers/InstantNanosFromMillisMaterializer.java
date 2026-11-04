//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit InstantNanosFromMicrosMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MAX_CONVERTIBLE_MILLIS;
import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MILLI;

public class InstantNanosFromMillisMaterializer extends LongMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new InstantNanosFromMillisMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new InstantNanosFromMillisMaterializer(dataReader, numValues);
        }
    };

    public static long convertValue(long value) {
        if (value > MAX_CONVERTIBLE_MILLIS || value < -MAX_CONVERTIBLE_MILLIS) {
            throw new UncheckedDeephavenException("Converting " + value + " millis to nanos would overflow");
        }
        return value * MILLI;
    }

    private final ValuesReader dataReader;
    /** {@link #dataReader} when the page is PLAIN-encoded, else null. */
    private final PlainLongValuesReader plainReader;

    private InstantNanosFromMillisMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private InstantNanosFromMillisMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
        this.plainReader = dataReader instanceof PlainLongValuesReader plain ? plain : null;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        if (plainReader != null) {
            final long[] scratch = plainReader.scratch();
            for (int ii = startIndex; ii < endIndex;) {
                final int length = Math.min(endIndex - ii, scratch.length);
                plainReader.readLongs(scratch, 0, length);
                for (int jj = 0; jj < length; jj++) {
                    data[ii + jj] = convertValue(scratch[jj]);
                }
                ii += length;
            }
            return;
        }
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readLong());
        }
    }
}
