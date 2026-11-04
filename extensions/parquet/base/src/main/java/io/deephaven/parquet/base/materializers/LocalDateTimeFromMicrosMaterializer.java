//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LocalDateTimeFromMillisMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MICRO;

public class LocalDateTimeFromMicrosMaterializer extends ObjectMaterializerBase<LocalDateTime>
        implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalDateTimeFromMicrosMaterializer(dataReader, (LocalDateTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalDateTimeFromMicrosMaterializer(dataReader, numValues);
        }
    };

    /**
     * Converts microseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     *
     * @param value microseconds since Epoch
     * @return The input microseconds from the Epoch converted to a {@link LocalDateTime} in UTC timezone
     */
    public static LocalDateTime convertValue(long value) {
        // Floor semantics, so that pre-Epoch values yield the non-negative nano-of-second that
        // LocalDateTime.ofEpochSecond requires.
        return LocalDateTime.ofEpochSecond(Math.floorDiv(value, 1_000_000),
                (int) (Math.floorMod(value, 1_000_000) * MICRO),
                ZoneOffset.UTC);
    }

    private final ValuesReader dataReader;
    /** {@link #dataReader} when the page is PLAIN-encoded, else null. */
    private final PlainLongValuesReader plainReader;

    private LocalDateTimeFromMicrosMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalDateTimeFromMicrosMaterializer(ValuesReader dataReader, LocalDateTime nullValue,
            int numValues) {
        super(nullValue, new LocalDateTime[numValues]);
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
