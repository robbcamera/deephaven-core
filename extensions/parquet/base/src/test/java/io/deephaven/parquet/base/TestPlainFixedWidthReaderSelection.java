//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.materializers.PlainValuesReaders;
import io.deephaven.parquet.base.materializers.StringMaterializer;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.plain.BooleanPlainValuesReader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Which pages {@code ColumnPageReaderImpl} reads with {@link PlainValuesReaders}. Unlike the String hook, this one is
 * total: those readers implement the full per-value interface, so any factory can consume them.
 */
class TestPlainFixedWidthReaderSelection {

    private static final ByteBuffer HEAP = ByteBuffer.allocate(64);
    private static final ByteBuffer DIRECT = ByteBuffer.allocateDirect(64);
    private static final List<PrimitiveTypeName> FIXED_WIDTH = List.of(
            PrimitiveTypeName.INT32, PrimitiveTypeName.INT64, PrimitiveTypeName.FLOAT, PrimitiveTypeName.DOUBLE);

    private static ColumnPageReaderImpl readerFor(
            final PrimitiveTypeName type, final PageMaterializerFactory factory) {
        final ColumnDescriptor descriptor = new ColumnDescriptor(
                new String[] {"c"}, Types.required(type).named("c"), 0, 0);
        return new ColumnPageReaderImpl("c", null, null, ctx -> ColumnChunkReader.NULL_DICTIONARY, factory,
                descriptor, URI.create("file:///test"), List.of(), 0L,
                new PageHeader(PageType.DATA_PAGE, 0, 0), 0);
    }

    @Test
    void plainPagesGetDeephavensReaderForAnyFactory() {
        for (final PrimitiveTypeName type : FIXED_WIDTH) {
            final Class<?> expected = PlainValuesReaders.make(type, HEAP).getClass();
            for (final PageMaterializerFactory factory : List.of(
                    PageMaterializerFactory.NULL_FACTORY, StringMaterializer.FACTORY)) {
                for (final ByteBuffer buffer : List.of(HEAP, DIRECT)) {
                    assertThat(readerFor(type, factory).getDataReader(Encoding.PLAIN, buffer, 0, null))
                            .as("type %s, direct %s", type, buffer.isDirect())
                            .isExactlyInstanceOf(expected);
                }
            }
        }
    }

    /** Other types keep parquet's PLAIN readers. */
    @Test
    void otherTypesUseParquetsReader() {
        assertThat(readerFor(PrimitiveTypeName.BOOLEAN, PageMaterializerFactory.NULL_FACTORY)
                .getDataReader(Encoding.PLAIN, HEAP, 0, null))
                .isInstanceOf(BooleanPlainValuesReader.class);
    }

    /** Encoding is per page; a dictionary page handed to a PLAIN reader would decode RLE bytes as values. */
    @Test
    void dictionaryPagesReachTheDictionaryPath() {
        // noinspection deprecation
        for (final Encoding encoding : List.of(Encoding.RLE_DICTIONARY, Encoding.PLAIN_DICTIONARY)) {
            final ColumnPageReaderImpl reader =
                    readerFor(PrimitiveTypeName.INT32, PageMaterializerFactory.NULL_FACTORY);
            // NULL_DICTIONARY means "no dictionary was loaded", so reaching that branch is itself the assertion.
            assertThatThrownBy(() -> reader.getDataReader(encoding, HEAP, 0, null))
                    .as("encoding %s", encoding)
                    .isInstanceOf(ParquetDecodingException.class);
        }
    }
}
