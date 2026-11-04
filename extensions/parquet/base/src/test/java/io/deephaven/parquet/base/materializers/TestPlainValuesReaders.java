//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestPlainValuesReaders {

    /** More than two scratch chunks, and not a multiple of one. */
    private static final int COUNT = 2 * PlainIntValuesReader.SCRATCH_SIZE + 452;

    /** Builds a PLAIN page with parquet's own writer, so the bytes are what a real page would hold. */
    private static byte[] encode(final Consumer<PlainValuesWriter> writes) {
        try (final PlainValuesWriter writer =
                new PlainValuesWriter(64, 64 * 1024, new HeapByteBufferAllocator())) {
            writes.accept(writer);
            return writer.getBytes().toByteArray();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] ints() {
        return encode(w -> {
            for (int ii = 0; ii < COUNT; ++ii) {
                // Mixes signs, and values that are negative as short, byte or char.
                w.writeInteger(ii % 2 == 0 ? ii * 104_729 : -ii * 7_919);
            }
        });
    }

    private static byte[] longs() {
        return encode(w -> {
            for (int ii = 0; ii < COUNT; ++ii) {
                w.writeLong(ii % 2 == 0 ? ii * 1_000_000_007L : -ii * 998_244_353L);
            }
        });
    }

    private static byte[] floats() {
        return encode(w -> {
            for (int ii = 0; ii < COUNT; ++ii) {
                w.writeFloat(ii == 7 ? Float.NaN : ii * -1.25f);
            }
        });
    }

    private static byte[] doubles() {
        return encode(w -> {
            for (int ii = 0; ii < COUNT; ++ii) {
                w.writeDouble(ii == 7 ? Double.NaN : ii * 3.5e100);
            }
        });
    }

    /** The real page buffer is oversized and reused, with the values somewhere in the middle. */
    private static ByteBuffer embedded(final byte[] encoded) {
        final byte[] oversized = new byte[encoded.length + 64];
        Arrays.fill(oversized, (byte) 0x7F);
        System.arraycopy(encoded, 0, oversized, 16, encoded.length);
        return ByteBuffer.wrap(oversized).position(16).limit(16 + encoded.length);
    }

    private static ValuesReader stock(final PrimitiveTypeName type, final byte[] encoded) {
        final ValuesReader reader = switch (type) {
            case INT32 -> new PlainValuesReader.IntegerPlainValuesReader();
            case INT64 -> new PlainValuesReader.LongPlainValuesReader();
            case FLOAT -> new PlainValuesReader.FloatPlainValuesReader();
            case DOUBLE -> new PlainValuesReader.DoublePlainValuesReader();
            default -> throw new IllegalArgumentException(type.toString());
        };
        try {
            reader.initFromPage(COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return reader;
    }

    private static ValuesReader plain(final PrimitiveTypeName type, final byte[] encoded) {
        final ValuesReader reader = PlainValuesReaders.make(type, embedded(encoded));
        assertThat(reader).isNotNull();
        return reader;
    }

    /**
     * Our reader with its per-value reads disabled. A materializer that drops its bulk path still produces the right
     * values through the per-value reads, so only this makes the loss fail a test.
     */
    private static ValuesReader bulkOnly(final PrimitiveTypeName type, final byte[] encoded) {
        final ByteBuffer in = embedded(encoded);
        return switch (type) {
            case INT32 -> new PlainIntValuesReader(in) {
                @Override
                public int readInteger() {
                    throw new AssertionError("per-value read");
                }
            };
            case INT64 -> new PlainLongValuesReader(in) {
                @Override
                public long readLong() {
                    throw new AssertionError("per-value read");
                }
            };
            case FLOAT -> new PlainFloatValuesReader(in) {
                @Override
                public float readFloat() {
                    throw new AssertionError("per-value read");
                }
            };
            case DOUBLE -> new PlainDoubleValuesReader(in) {
                @Override
                public double readDouble() {
                    throw new AssertionError("per-value read");
                }
            };
            default -> throw new IllegalArgumentException(type.toString());
        };
    }

    /**
     * Materialize a page with runs of values separated by nulls, as {@code materializeWithNulls} and the sparse cursor
     * do, once with parquet's reader and once with ours, which must take the bulk path. After every fifth run the
     * reader also skips a value, as the sparse cursor skips the rows it doesn't want; the gap is null-filled either
     * way.
     */
    private static void assertMaterializesLikeStock(
            final PageMaterializerFactory factory, final Object nullValue,
            final PrimitiveTypeName type, final byte[] encoded) {
        final Object expected = materialize(factory, nullValue, stock(type, encoded));
        final Object actual = materialize(factory, nullValue, bulkOnly(type, encoded));
        assertThat(actual).isEqualTo(expected);
    }

    private static Object materialize(
            final PageMaterializerFactory factory, final Object nullValue, final ValuesReader reader) {
        final PageMaterializer materializer = factory.makeMaterializerWithNulls(reader, nullValue, COUNT);
        int pos = 0;
        int run = 1;
        while (pos < COUNT) {
            final int end = Math.min(COUNT, pos + run);
            materializer.fillValues(pos, end);
            pos = end;
            if (pos < COUNT) {
                if (run % 5 == 0) {
                    reader.skip(1);
                }
                materializer.fillNulls(pos, pos + 1);
                ++pos;
            }
            run = run % 13 + 1;
        }
        return materializer.data();
    }

    @Test
    void perValueReadsMatchStock() {
        final ValuesReader stockInts = stock(PrimitiveTypeName.INT32, ints());
        final ValuesReader plainInts = plain(PrimitiveTypeName.INT32, ints());
        final ValuesReader stockLongs = stock(PrimitiveTypeName.INT64, longs());
        final ValuesReader plainLongs = plain(PrimitiveTypeName.INT64, longs());
        final ValuesReader stockFloats = stock(PrimitiveTypeName.FLOAT, floats());
        final ValuesReader plainFloats = plain(PrimitiveTypeName.FLOAT, floats());
        final ValuesReader stockDoubles = stock(PrimitiveTypeName.DOUBLE, doubles());
        final ValuesReader plainDoubles = plain(PrimitiveTypeName.DOUBLE, doubles());
        for (int ii = 0; ii < COUNT; ++ii) {
            if (ii % 7 == 3) {
                for (final ValuesReader reader : List.of(stockInts, plainInts, stockLongs, plainLongs,
                        stockFloats, plainFloats, stockDoubles, plainDoubles)) {
                    reader.skip();
                }
                continue;
            }
            assertThat(plainInts.readInteger()).isEqualTo(stockInts.readInteger());
            assertThat(plainLongs.readLong()).isEqualTo(stockLongs.readLong());
            assertThat(Float.floatToRawIntBits(plainFloats.readFloat()))
                    .isEqualTo(Float.floatToRawIntBits(stockFloats.readFloat()));
            assertThat(Double.doubleToRawLongBits(plainDoubles.readDouble()))
                    .isEqualTo(Double.doubleToRawLongBits(stockDoubles.readDouble()));
        }
    }

    @Test
    void identityMaterializersMatchStock() {
        assertMaterializesLikeStock(IntMaterializer.FACTORY, Integer.MIN_VALUE, PrimitiveTypeName.INT32, ints());
        assertMaterializesLikeStock(LongMaterializer.FACTORY, Long.MIN_VALUE, PrimitiveTypeName.INT64, longs());
        assertMaterializesLikeStock(FloatMaterializer.FACTORY, -Float.MAX_VALUE, PrimitiveTypeName.FLOAT, floats());
        assertMaterializesLikeStock(DoubleMaterializer.FACTORY, -Double.MAX_VALUE, PrimitiveTypeName.DOUBLE,
                doubles());
    }

    /** One materializer per template the replicator expands, so a bad replication pair fails here. */
    @Test
    void convertingMaterializersMatchStock() {
        final byte[] ints = ints();
        assertMaterializesLikeStock(ShortMaterializer.FACTORY, Short.MIN_VALUE, PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(ByteMaterializer.FACTORY, Byte.MIN_VALUE, PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(CharMaterializer.FACTORY, Character.MAX_VALUE, PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(LongFromIntMaterializer.FACTORY, Long.MIN_VALUE, PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(LongFromUnsignedIntMaterializer.FACTORY, Long.MIN_VALUE,
                PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(IntFromUnsignedShortMaterializer.FACTORY, Integer.MIN_VALUE,
                PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(ShortFromUnsignedByteMaterializer.FACTORY, Short.MIN_VALUE,
                PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(LocalDateMaterializer.FACTORY, null, PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(new BigDecimalFromIntMaterializer.Factory(3), null,
                PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(DoubleFromFloatMaterializer.FACTORY, -Double.MAX_VALUE,
                PrimitiveTypeName.FLOAT, floats());

        // In range for the time conversions, and non-negative for the unsigned check.
        final byte[] times = encode(w -> {
            for (int ii = 0; ii < COUNT; ++ii) {
                w.writeLong(ii % 1_000 * 86_399_999L);
            }
        });
        assertMaterializesLikeStock(LongFromUnsignedLongMaterializer.FACTORY, Long.MIN_VALUE,
                PrimitiveTypeName.INT64, times);
        assertMaterializesLikeStock(InstantNanosFromMicrosMaterializer.FACTORY, Long.MIN_VALUE,
                PrimitiveTypeName.INT64, times);
        assertMaterializesLikeStock(LocalTimeFromMicrosMaterializer.FACTORY, null, PrimitiveTypeName.INT64, times);
        assertMaterializesLikeStock(LocalDateTimeFromMillisMaterializer.FACTORY, null,
                PrimitiveTypeName.INT64, times);
        assertMaterializesLikeStock(new BigDecimalFromLongMaterializer.Factory(2), null,
                PrimitiveTypeName.INT64, longs());
    }

    /** The remaining generated materializers, so each one's bulk path is covered too. */
    @Test
    void otherGeneratedMaterializersMatchStock() {
        final byte[] ints = ints();
        assertMaterializesLikeStock(LongFromUnsignedShortMaterializer.FACTORY, Long.MIN_VALUE,
                PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(LongFromUnsignedByteMaterializer.FACTORY, Long.MIN_VALUE,
                PrimitiveTypeName.INT32, ints);
        assertMaterializesLikeStock(IntFromUnsignedByteMaterializer.FACTORY, Integer.MIN_VALUE,
                PrimitiveTypeName.INT32, ints);

        final byte[] times = encode(w -> {
            for (int ii = 0; ii < COUNT; ++ii) {
                w.writeLong(ii % 1_000 * 86_399_999L);
            }
        });
        assertMaterializesLikeStock(InstantNanosFromMillisMaterializer.FACTORY, Long.MIN_VALUE,
                PrimitiveTypeName.INT64, times);
        assertMaterializesLikeStock(LocalTimeFromNanosMaterializer.FACTORY, null, PrimitiveTypeName.INT64, times);
        assertMaterializesLikeStock(LocalDateTimeFromMicrosMaterializer.FACTORY, null,
                PrimitiveTypeName.INT64, times);
        assertMaterializesLikeStock(LocalDateTimeFromNanosMaterializer.FACTORY, null,
                PrimitiveTypeName.INT64, times);

        // Milliseconds of the day fit in an int.
        assertMaterializesLikeStock(LocalTimeFromMillisMaterializer.FACTORY, null, PrimitiveTypeName.INT32,
                encode(w -> {
                    for (int ii = 0; ii < COUNT; ++ii) {
                        w.writeInteger(ii % 1_000 * 86_399);
                    }
                }));
    }

    @Test
    void materializedValuesAreTheWrittenOnes() {
        final int[] actual = (int[]) IntMaterializer.FACTORY
                .makeMaterializerNonNull(plain(PrimitiveTypeName.INT32, ints()), COUNT).fillAll();
        for (int ii = 0; ii < COUNT; ++ii) {
            assertThat(actual[ii]).isEqualTo(ii % 2 == 0 ? ii * 104_729 : -ii * 7_919);
        }
        final LocalDate[] dates = (LocalDate[]) LocalDateMaterializer.FACTORY
                .makeMaterializerNonNull(plain(PrimitiveTypeName.INT32, encode(w -> w.writeInteger(19_000))), 1)
                .fillAll();
        assertThat(dates).containsExactly(LocalDate.ofEpochDay(19_000));
        final BigDecimal[] decimals = (BigDecimal[]) new BigDecimalFromLongMaterializer.Factory(2)
                .makeMaterializerNonNull(plain(PrimitiveTypeName.INT64, encode(w -> w.writeLong(-12_345))), 1)
                .fillAll();
        assertThat(decimals).containsExactly(new BigDecimal("-123.45"));
    }

    /**
     * Because the page buffer is reused and over-allocated, an unchecked overrun would return bytes from the previous
     * page rather than failing.
     */
    @Test
    void readingPastTheLimitThrows() {
        final byte[] encoded = ints();
        final List<Supplier<Object>> overruns = List.of(
                () -> {
                    final ValuesReader reader = plain(PrimitiveTypeName.INT32, encoded);
                    reader.skip(COUNT);
                    return reader.readInteger();
                },
                () -> {
                    plain(PrimitiveTypeName.INT32, encoded).skip(COUNT + 1);
                    return null;
                },
                () -> IntMaterializer.FACTORY
                        .makeMaterializerNonNull(plain(PrimitiveTypeName.INT32, encoded), COUNT + 1).fillAll(),
                // An empty page: the converting copy loop must fail, not spin on an empty scratch buffer.
                () -> {
                    ShortMaterializer.FACTORY.makeMaterializerNonNull(plain(PrimitiveTypeName.INT32, new byte[0]), 0)
                            .fillValues(0, 1);
                    return null;
                },
                // A page whose length is not a whole number of values.
                () -> LongMaterializer.FACTORY.makeMaterializerNonNull(
                        PlainValuesReaders.make(PrimitiveTypeName.INT64, ByteBuffer.wrap(new byte[12])), 2)
                        .fillAll());
        for (final Supplier<Object> overrun : overruns) {
            assertThatThrownBy(overrun::get).isInstanceOf(ParquetDecodingException.class);
        }
    }

    @Test
    void onlyFixedWidthTypesHaveAReader() {
        final ByteBuffer heap = ByteBuffer.allocate(16);
        for (final PrimitiveTypeName type : List.of(PrimitiveTypeName.BOOLEAN, PrimitiveTypeName.BINARY,
                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, PrimitiveTypeName.INT96)) {
            assertThat(PlainValuesReaders.make(type, heap)).as("type %s", type).isNull();
        }
    }

    @Test
    void readsDirectBuffers() {
        final byte[] encoded = longs();
        final ByteBuffer direct = ByteBuffer.allocateDirect(encoded.length + 3);
        direct.position(3);
        direct.put(encoded).flip().position(3);
        final long[] actual = (long[]) LongMaterializer.FACTORY
                .makeMaterializerNonNull(PlainValuesReaders.make(PrimitiveTypeName.INT64, direct), COUNT).fillAll();
        final long[] expected = (long[]) LongMaterializer.FACTORY
                .makeMaterializerNonNull(stock(PrimitiveTypeName.INT64, encoded), COUNT).fillAll();
        assertThat(actual).isEqualTo(expected);
    }
}
