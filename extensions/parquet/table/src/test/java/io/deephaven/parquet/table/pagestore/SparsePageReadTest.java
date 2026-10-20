//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.base.FileUtils;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortOperation;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.page.RowsOfInterest;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.metadata.RowGroupInfo;
import io.deephaven.util.SafeCloseable;
import io.deephaven.vector.Vector;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Sparse reads must return exactly what whole-page materialization returns.
 * <p>
 * Tests that assert a read is served from what an earlier read cached pin the cache with
 * {@link PageCache#pinCreatedPages()}. The cache holds pages softly, so a GC between the reads that clears soft
 * references would make the later read decode again and fail the assertion.
 */
public class SparsePageReadTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final int NUM_ROWS = 200_000;

    private static final String[] FILTERS = {
            "ii % 13 == 0",
            "ii % 997 == 3",
            "ii % 20011 == 0",
            "ii < 100",
            "ii >= 99990 && ii < 130010",
            "ii % 5000 < 3",
    };

    private File dataDirectory;
    private double oldMaxDensity;
    private int oldMissesBeforeFullCaching;

    @Before
    public void setUp() throws IOException {
        dataDirectory = Files.createTempDirectory(SparsePageReadTest.class.getName()).toFile();
        // Take the sparse path for every uncached page.
        oldMaxDensity = ColumnChunkPageStore.setSparseReadMaxDensity(1.0);
        oldMissesBeforeFullCaching = ColumnChunkPageStore.setSparseMissesBeforeFullCaching(Integer.MAX_VALUE);
    }

    @After
    public void tearDown() {
        ColumnChunkPageStore.setSparseReadMaxDensity(oldMaxDensity);
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(oldMissesBeforeFullCaching);
        if (dataDirectory.exists()) {
            TrackedFileHandleFactory.getInstance().closeAll();
            FileUtils.deleteRecursively(dataDirectory);
        }
    }

    /**
     * Most column types, with scattered nulls and a null run spanning several pages. {@code Str} has too many values
     * for a dictionary, so it is PLAIN; {@code Sym} is dictionary-encoded.
     */
    private String writeTestTable() {
        final Table table = TableTools.emptyTable(NUM_ROWS).update(
                "IsNull = ii % 7 == 0 || (ii >= 100000 && ii < 130000)",
                "B = IsNull ? NULL_BYTE : (byte) ii",
                "C = IsNull ? NULL_CHAR : (char) ('A' + ii % 26)",
                "S = IsNull ? NULL_SHORT : (short) ii",
                "I = IsNull ? NULL_INT : (int) ii",
                "L = IsNull ? NULL_LONG : ii",
                "F = IsNull ? NULL_FLOAT : (float) ii",
                "D = IsNull ? NULL_DOUBLE : ii / 3.0",
                "Str = IsNull ? null : `value-` + ii",
                "Sym = IsNull ? null : `sym-` + (ii % 50)",
                "BD = IsNull ? null : java.math.BigDecimal.valueOf(ii, 2)",
                "Inst = IsNull ? null : DateTimeUtils.epochNanosToInstant(ii * 1000)",
                "LD = IsNull ? null : java.time.LocalDate.ofEpochDay(ii % 10000)",
                "Arr = IsNull ? null : new int[] {(int) ii, (int) ii + 1}");
        final String path = new File(dataDirectory, "table.parquet").getPath();
        ParquetTools.writeTable(table, path);
        return path;
    }

    @Test
    public void testFiltersMatchDense() {
        assertSparseMatchesDense(writeTestTable(), true);
    }

    /**
     * Formulas read their inputs with {@code getChunk}, which must read sparsely like {@code fillChunk}: for primitive
     * columns, and for {@code Instant} and {@code Boolean} columns read as objects through their native regions.
     */
    @Test
    public void testGetChunkReadsSparsely() {
        final String path = writeTestTable();
        final String[][] directAndFormula = {
                {"L", "X = L + 1"},
                {"Inst", "X = isNull(Inst) ? NULL_LONG : epochNanos(Inst)"},
                {"IsNull", "X = IsNull == true"},
        };
        for (final String[] pair : directAndFormula) {
            final long[] fills = new long[2];
            for (int ii = 0; ii < 2; ++ii) {
                final String column = pair[ii];
                final Table expected =
                        dense(() -> ParquetTools.readTable(path).where(FILTERS[0]).view(column).select());
                final long before = ColumnChunkPageStore.sparseFillCount();
                // A fresh read, so that no page is already cached.
                assertTableEquals(expected, ParquetTools.readTable(path).where(FILTERS[0]).view(column));
                fills[ii] = ColumnChunkPageStore.sparseFillCount() - before;
            }
            assertTrue(pair[0] + " should read sparsely", fills[0] > 0);
            assertEquals(pair[1] + " should read as sparsely as " + pair[0], fills[0], fills[1]);
        }
    }

    @Test
    public void testReferenceFilesMatchDense() {
        // The V1/V2 and mixed-encoding files are not written by Deephaven, and the latter cover both page stores.
        assertSparseMatchesDense(resource("/ReferenceParquetV1PageData.parquet"), true);
        assertSparseMatchesDense(resource("/ReferenceParquetV2PageData.parquet"), true);
        assertSparseMatchesDense(resource("/ParquetDataWithMixedEncodingWithOffsetIndex.parquet"), true);
        assertSparseMatchesDense(resource("/ParquetDataWithMixedEncodingWithoutOffsetIndex.parquet"), true);
        // Non-nullable columns, so no definition levels: V1 with an offset index; V2 without one, with uncompressed
        // pages in a snappy column chunk.
        assertSparseMatchesDense(resource("/ReferenceRequiredColumnsV1.parquet"), true);
        assertSparseMatchesDense(resource("/ReferenceRequiredColumnsV2.parquet"), true);
        assertSparseMatchesDense(resource("/ReferenceNullStringDictEncoded1.parquet"), true);
        // Repeated columns only, so these must fall back to materializing pages.
        assertSparseMatchesDense(resource("/ReferenceParquetFileWithDifferentPageSizes1.parquet"), false);
        assertSparseMatchesDense(resource("/ReferenceParquetArrayData.parquet"), false);
        assertSparseMatchesDense(resource("/ReferenceParquetVectorData.parquet"), false);
    }

    /**
     * Rebuilds {@code ReferenceRequiredColumnsV1.parquet} in {@code src/test/resources}: non-nullable columns in 4 KiB
     * snappy V1 pages with an offset index, dictionary encoded only for {@code Sym}. Remove the {@link Ignore} to run
     * it. {@code ReferenceRequiredColumnsV2.py} writes the V2 file, which parquet-java can't.
     */
    @Ignore("Generates a checked-in test resource")
    @Test
    public void generateRequiredColumnsV1File() throws IOException {
        final int numRows = 5_000;
        final MessageType schema = Types.buildMessage()
                .required(PrimitiveTypeName.INT32).named("I")
                .required(PrimitiveTypeName.INT64).named("L")
                .required(PrimitiveTypeName.DOUBLE).named("D")
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("Str")
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("Sym")
                .named("schema");
        final SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        // The test's working directory is the project's.
        final File dest = new File("src/test/resources", "ReferenceRequiredColumnsV1.parquet");
        try (final ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(new LocalOutputFile(dest.toPath()))
                .withType(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withWriterVersion(WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(4 * 1024)
                .withDictionaryEncoding(false)
                .withDictionaryEncoding("Sym", true)
                .build()) {
            for (int ii = 0; ii < numRows; ++ii) {
                final Group group = groupFactory.newGroup();
                group.add("I", ii);
                group.add("L", ii * 3L);
                group.add("D", ii / 3.0);
                group.add("Str", "value-" + ii);
                group.add("Sym", "sym-" + ii % 50);
                writer.write(group);
            }
        }
    }

    @Test
    public void testCachedPagesAreReused() {
        final String path = writeTestTable();
        final Table source = ParquetTools.readTable(path);
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final Table dense = dense(() -> source.select());
            final long before = ColumnChunkPageStore.sparseFillCount();
            assertTableEquals(dense.where(FILTERS[0]), source.where(FILTERS[0]));
            assertEquals("pages materialized by the dense read serve the sparse one",
                    before, ColumnChunkPageStore.sparseFillCount());
        }
    }

    /**
     * Small chunks split each page across many fills. In order, each fill resumes where the previous one stopped.
     * Reversed, each starts before the previous one on the same context, so only each column's first fill reads
     * sparsely; the rest materialize and cache their pages.
     */
    @Test
    public void testChunkedFills() {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final RowSet rows = expected.where("ii % 11 == 0").getRowSet();
        long forwardFills = 0;
        for (final boolean reverse : new boolean[] {false, true}) {
            final Table actual = ParquetTools.readTable(path);
            final long fillsBefore = ColumnChunkPageStore.sparseFillCount();
            final long opensBefore = ColumnChunkPageStore.sparseOpenCount();
            for (final String column : expected.getDefinition().getColumnNamesArray()) {
                assertFillsMatch(column, expected.getColumnSource(column), actual.getColumnSource(column), rows,
                        reverse);
            }
            final long fills = ColumnChunkPageStore.sparseFillCount() - fillsBefore;
            final long opens = ColumnChunkPageStore.sparseOpenCount() - opensBefore;
            assertTrue("fills should read sparsely", fills > 0);
            if (reverse) {
                assertTrue("reversed fills go dense: " + fills + " sparse fills, against " + forwardFills
                        + " in order", fills * 100 < forwardFills);
            } else {
                assertTrue("in-order fills resume: " + opens + " opens for " + fills + " fills", opens * 10 < fills);
                forwardFills = fills;
            }
        }
    }

    /**
     * A context publishes the rows it decoded from its last page when it closes.
     */
    @Test
    public void testPendingPublishedOnClose() {
        final String path = writeTestTable();
        final String filter = "ii < 4000 && ii % 13 == 0";
        final Table expected = dense(() -> ParquetTools.readTable(path).where(filter).select());
        final Table filtered = ParquetTools.readTable(path).where(filter);
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            assertPass("first", expected, filtered, Reads.DECODE);
            assertPass("second", expected, filtered, Reads.HIT);
        }
    }

    /**
     * A fill context crosses row groups, and so page stores, publishing each store's rows as it leaves it.
     */
    @Test
    public void testRowGroupsShareContext() {
        final Table table = TableTools.emptyTable(NUM_ROWS).update("L = ii", "Str = `value-` + ii");
        final String path = new File(dataDirectory, "rowGroups.parquet").getPath();
        ParquetTools.writeTable(table, path,
                ParquetInstructions.builder().setRowGroupInfo(RowGroupInfo.maxRows(30_000)).build());
        final Table expected = dense(() -> ParquetTools.readTable(path).where(FILTERS[0]).select());
        final Table filtered = ParquetTools.readTable(path).where(FILTERS[0]);
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            assertPass("first", expected, filtered, Reads.DECODE);
            assertPass("second", expected, filtered, Reads.HIT);
        }
    }

    /**
     * Pages that don't start at a multiple of the RowSet block size, read at about 10% density in fills large enough
     * that their rows are stored as bitmaps. Shifting a RowSet per fill, or appending one bitmap to another, once
     * allocated quadratically in the rows per block.
     */
    @Test
    public void testUnalignedPagesAllocateLinearly() {
        final int numRows = 900_000;
        final Table table = TableTools.emptyTable(numRows).update("L = ii");
        final String path = new File(dataDirectory, "unaligned.parquet").getPath();
        // 300,000 longs per page, so every page after the first starts unaligned, and each takes several fills.
        ParquetTools.writeTable(table, path, ParquetInstructions.builder().setTargetPageSize(2_400_000).build());
        ColumnChunkPageStore.setSparseReadMaxDensity(0.125);
        final ColumnSource<?> expected = dense(() -> ParquetTools.readTable(path).select()).getColumnSource("L");
        final ColumnSource<?> actual = ParquetTools.readTable(path).getColumnSource("L");
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long ii = 0; ii < numRows; ii += 10) {
            builder.appendKey(ii);
        }
        try (final RowSet rows = builder.build();
                final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final long valueBytes = Long.BYTES * rows.size();
            final long denseAllocated = dense(() -> allocatedByFills(ParquetTools.readTable(path).getColumnSource("L"),
                    rows));
            for (final String pass : new String[] {"decode", "hit"}) {
                final long fillsBefore = ColumnChunkPageStore.sparseFillCount();
                final long hitsBefore = ColumnChunkPageStore.sparseHitCount();
                final long allocated = allocatedByFills(actual, rows);
                final long fills = ColumnChunkPageStore.sparseFillCount() - fillsBefore;
                final long hits = ColumnChunkPageStore.sparseHitCount() - hitsBefore;
                assertTrue(pass + ": " + fills + " sparse fills, " + hits + " hits",
                        pass.equals("decode") ? fills > 0 : fills == 0 && hits > 0);
                if (pass.equals("decode")) {
                    // About 2/3 of a dense read.
                    assertTrue("decodes allocated " + allocated + " bytes, against " + denseAllocated + " dense",
                            allocated < denseAllocated);
                } else {
                    // About 1/8 of the value bytes, all per fill rather than per row.
                    assertTrue("hits allocated " + allocated + " bytes for " + valueBytes + " bytes of values",
                            allocated < valueBytes / 2);
                }
            }
            assertFillsMatch("L", expected, actual, rows, false);
        }
    }

    /**
     * A fill served from a cached page allocates as it would without sparse reads: it makes no channel context or
     * sparse read state, which only an uncached page needs.
     */
    @Test
    public void testCachedPageFillsAllocateAsWithoutSparseReads() {
        final String path = writeTestTable();
        try (final SafeCloseable ignored = PageCache.pinCreatedPages();
                final RowSet row = RowSetFactory.fromKeys(5)) {
            final ColumnSource<?> sparse = ParquetTools.readTable(path).getColumnSource("L");
            final ColumnSource<?> withoutSparse = dense(() -> ParquetTools.readTable(path).coalesce())
                    .getColumnSource("L");
            // Cache the pages whole.
            dense(() -> allocatedByFills(sparse, row));
            dense(() -> allocatedByFills(withoutSparse, row));
            final int fills = 20_000;
            long sparseBytes = 0;
            long withoutSparseBytes = 0;
            // Warm, then measure.
            for (int pass = 0; pass < 2; ++pass) {
                sparseBytes = allocatedByContextFills(sparse, row, fills);
                withoutSparseBytes = dense(() -> allocatedByContextFills(withoutSparse, row, fills));
            }
            final String perFill = "per fill: " + (double) sparseBytes / fills + " bytes with sparse reads, "
                    + (double) withoutSparseBytes / fills + " without";
            assertTrue(perFill, sparseBytes <= withoutSparseBytes + 8L * fills);
            // About the context alone; a channel context and its wrapper would add over 150.
            assertTrue(perFill, withoutSparseBytes <= 64L * fills);
        }
    }

    /**
     * @return The bytes allocated by {@code fills} fills of {@code rows}, each with a fresh context
     */
    private static long allocatedByContextFills(final ColumnSource<?> source, final RowSet rows, final int fills) {
        final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long threadId = Thread.currentThread().getId();
        try (final WritableChunk<Values> chunk = source.getChunkType().makeWritableChunk(rows.intSize())) {
            final long before = threads.getThreadAllocatedBytes(threadId);
            for (int ii = 0; ii < fills; ++ii) {
                try (final ChunkSource.FillContext context = source.makeFillContext(rows.intSize())) {
                    source.fillChunk(context, chunk, rows);
                }
            }
            return threads.getThreadAllocatedBytes(threadId) - before;
        }
    }

    private static long allocatedByFills(final ColumnSource<?> source, final RowSet rows) {
        // Several fills per page, each with enough rows to be stored as a bitmap, appending to the rows pending for it.
        final int chunkSize = 8192;
        final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long threadId = Thread.currentThread().getId();
        try (final ChunkSource.FillContext context = source.makeFillContext(chunkSize);
                final WritableChunk<Values> chunk = source.getChunkType().makeWritableChunk(chunkSize);
                final RowSequence.Iterator it = rows.getRowSequenceIterator()) {
            final long before = threads.getThreadAllocatedBytes(threadId);
            while (it.hasMore()) {
                source.fillChunk(context, chunk, it.getNextRowSequenceWithLength(chunkSize));
            }
            return threads.getThreadAllocatedBytes(threadId) - before;
        }
    }

    /**
     * Later passes over the same rows are served from the values the first pass cached, and don't count toward
     * promotion.
     */
    @Test
    public void testRepeatedReadsHitValueCache() {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).where(FILTERS[0]).select());
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(1);
        final Table filtered = ParquetTools.readTable(path).where(FILTERS[0]);
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            assertPass("first", expected, filtered, Reads.DECODE);
            assertPass("second", expected, filtered, Reads.HIT);
            assertPass("third", expected, filtered, Reads.HIT);
        }
    }

    /**
     * Rows that are a subset of a page's cached values are served from them, whether near each other or far apart.
     */
    @Test
    public void testSubsetReadsHit() {
        final String path = writeTestTable();
        final Table source = ParquetTools.readTable(path);
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            // Decodes and caches every 13th row.
            assertPass("superset", dense(() -> ParquetTools.readTable(path).where("ii % 13 == 0").select()),
                    source.where("ii % 13 == 0"), Reads.DECODE);
            // Every other cached row: each seek steps over one cached range.
            assertPass("subset", dense(() -> ParquetTools.readTable(path).where("ii % 26 == 0").select()),
                    source.where("ii % 26 == 0"), Reads.HIT);
            // Every 100th cached row: each seek skips past several checkpoints, so it jumps to the last one before it.
            assertPass("far subset", dense(() -> ParquetTools.readTable(path).where("ii % 1300 == 0").select()),
                    source.where("ii % 1300 == 0"), Reads.HIT);
        }
    }

    /**
     * A pass that needs rows the cache lacks decodes just those, and merges them into the cache.
     */
    @Test
    public void testMergeThenHit() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(2);
        final Table source = ParquetTools.readTable(path);
        final String[] filters = {"ii % 26 == 0", "ii % 26 == 13", "ii % 13 == 0"};
        final Reads[] reads = {Reads.DECODE, Reads.DECODE, Reads.HIT};
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            assertPasses(path, source, filters, reads);
        }
    }

    /**
     * Requested ranges that start, end, or both, inside cached runs, including runs joined across the boundary between
     * hundreds.
     */
    @Test
    public void testRangesStraddleCachedRuns() {
        final String path = writeTestTable();
        final Table source = ParquetTools.readTable(path);
        final String[] filters = {
                // Cached: [0, 9] of every 100.
                "ii % 100 < 10",
                // Cached, then missing.
                "ii % 100 >= 5 && ii % 100 < 15",
                // Missing, cached, missing: [90, 119] across each boundary between hundreds.
                "ii % 100 >= 90 || ii % 100 < 20",
                // Within one cached run.
                "ii % 100 >= 3 && ii % 100 < 17",
                // Inside the joined runs at both ends.
                "ii % 100 >= 95 || ii % 100 < 18",
        };
        final Reads[] reads = {Reads.DECODE, Reads.DECODE, Reads.DECODE, Reads.HIT, Reads.HIT};
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            assertPasses(path, source, filters, reads);
        }
    }

    /**
     * Merging stops, and the page is cached whole, once the cached rows would exceed the density cutoff.
     */
    @Test
    public void testMergePromotesOnDensity() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseReadMaxDensity(0.05);
        final Table source = ParquetTools.readTable(path);
        // Each filter alone is under the cutoff; together they are over it. The second pass may decode part of a page
        // before crossing the cutoff, but then caches the page whole, so the third pass reads whole pages.
        final String first = "ii % 26 == 0";
        final Table expected = dense(() -> ParquetTools.readTable(path).where(first).select());
        // Select serially. A parallel select splits a page among contexts, each of which checks the cutoff against only
        // the published rows and its own, so none may promote the page, and the merged values exceed the cutoff.
        final long oldMinimumParallelSelectRows = QueryTable.MINIMUM_PARALLEL_SELECT_ROWS;
        QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = Long.MAX_VALUE;
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            assertPass("first", expected, source.where(first), Reads.DECODE);
            final String second = "ii % 26 == 13";
            final long declinesBefore = ColumnChunkPageStore.sparseDeclineCount();
            assertPass("second", dense(() -> ParquetTools.readTable(path).where(second).select()),
                    source.where(second), Reads.ANY);
            assertTrue("the cutoff should decline the second pass",
                    ColumnChunkPageStore.sparseDeclineCount() > declinesBefore);
            assertPass("third", expected, source.where(first), Reads.DENSE);
        } finally {
            QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = oldMinimumParallelSelectRows;
        }
    }

    /**
     * Merging stops, and the page is cached whole, after {@code sparseMissesBeforeFullCaching} misses.
     */
    @Test
    public void testMergePromotesOnLimit() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(2);
        final Table source = ParquetTools.readTable(path);
        final String[] filters = {"ii % 39 == 0", "ii % 39 == 13", "ii % 39 == 26", "ii % 39 == 0"};
        final Reads[] reads = {Reads.DECODE, Reads.DECODE, Reads.DENSE, Reads.DENSE};
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            assertPasses(path, source, filters, reads);
        }
    }

    /**
     * Reopening a page because its cached values were evicted is not a repeated pass: a single reader of the same rows
     * keeps reading sparsely, however often memory pressure evicts them.
     */
    @Test
    public void testEvictionResetsMisses() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(2);
        final Table expected = dense(() -> ParquetTools.readTable(path).where(FILTERS[0]).select());
        final Table filtered = ParquetTools.readTable(path).where(FILTERS[0]);
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            assertPass("first", expected, filtered, Reads.DECODE);
            for (int pass = 2; pass <= 4; ++pass) {
                PageCache.evictPinnedPages();
                System.gc();
                assertPass("after eviction " + (pass - 1), expected, filtered, Reads.DECODE);
            }
        }
    }

    /**
     * A viewport scrolling in file order without a hint, with a fresh context per step, reopens the page the previous
     * step ended on, decompressing it again. Those reopens are misses, so a page many steps land on is cached whole.
     */
    @Test
    public void testForwardScrollMisses() {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages();
                final RowSet rows = expected.where("ii % 13 == 0").getRowSet().copy()) {
            final ColumnSource<?> expectedSource = expected.getColumnSource("L");
            final long[] neverPromoted =
                    fillSteps(expectedSource, ParquetTools.readTable(path).getColumnSource("L"), rows, 50);
            ColumnChunkPageStore.setSparseMissesBeforeFullCaching(1);
            final ColumnSource<?> actual = ParquetTools.readTable(path).getColumnSource("L");
            final long[] limitOfOne = fillSteps(expectedSource, actual, rows, 50);
            // Dozens of steps land on each page; with a limit of one, a page's second open caches it whole.
            assertTrue("sparse fills " + limitOfOne[0] + " vs " + neverPromoted[0],
                    limitOfOne[0] * 5 < neverPromoted[0]);
            final long[] again = fillSteps(expectedSource, actual, rows, 50);
            assertEquals("sparse fills after promotion", 0, again[0]);
            assertEquals("sparse hits after promotion", 0, again[1]);
        }
    }

    /**
     * A request dense enough to cache its page whole is served from the page's cached sparse values if they cover it,
     * rather than reading the page again.
     */
    @Test
    public void testDenseRequestHitsCachedValues() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseReadMaxDensity(0.1);
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final ColumnSource<?> expectedSource = expected.getColumnSource("L");
        final ColumnSource<?> actual = ParquetTools.readTable(path).getColumnSource("L");
        try (final SafeCloseable ignored = PageCache.pinCreatedPages();
                final RowSet cached = expected.where("ii % 13 == 0").getRowSet().copy();
                final RowSet single = expected.where("ii % 1300 == 0").getRowSet().copy()) {
            assertTrue("decodes", fillSteps(expectedSource, actual, cached, cached.intSize())[0] > 0);
            // One row per fill: dense over its span of one row.
            final long declinesBefore = ColumnChunkPageStore.sparseDeclineCount();
            final long[] reads = fillSteps(expectedSource, actual, single, 1);
            assertEquals("sparse fills", 0, reads[0]);
            assertEquals("sparse hits", single.size(), reads[1]);
            assertEquals("sparse declines", declinesBefore, ColumnChunkPageStore.sparseDeclineCount());
        }
    }

    /**
     * A fill that starts on a page behind rows requested there is a miss.
     */
    @Test
    public void testReadBehindIsMiss() {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(1);
        final ColumnSource<?> expectedSource = expected.getColumnSource("L");
        final ColumnSource<?> actual = ParquetTools.readTable(path).getColumnSource("L");
        try (final SafeCloseable ignored = PageCache.pinCreatedPages();
                final RowSet rows = expected.where("ii % 13 == 0").getRowSet().copy();
                final RowSet behind = expected.where("ii % 13 == 1").getRowSet().copy()) {
            // A single fill: one miss per page.
            final long[] first = fillSteps(expectedSource, actual, rows, rows.intSize());
            assertTrue("sparse fills", first[0] > 0);
            // Another single fill, behind the rows read on each page: a second miss per page, which caches every page
            // whole.
            fillSteps(expectedSource, actual, behind, behind.intSize());
            final long[] reads = fillSteps(expectedSource, actual, rows, rows.intSize());
            assertEquals("sparse fills", 0, reads[0]);
            assertEquals("sparse hits", 0, reads[1]);
        }
    }

    /**
     * Fill {@code rows} in steps of {@code step}, each with a fresh context, as viewport snapshots do, and check them
     * against {@code expected}.
     *
     * @return The sparse fills and hits
     */
    private static long[] fillSteps(final ColumnSource<?> expected, final ColumnSource<?> actual, final RowSet rows,
            final int step) {
        return fillSteps(expected, rows, actual, rows, step);
    }

    /**
     * As {@link #fillSteps(ColumnSource, ColumnSource, RowSet, int)}, reading {@code expected} at {@code expectedRows}
     * and {@code actual} at the rows in the same positions of {@code actualRows}.
     */
    private static long[] fillSteps(final ColumnSource<?> expected, final RowSet expectedRows,
            final ColumnSource<?> actual, final RowSet actualRows, final int step) {
        assertEquals("rows", expectedRows.size(), actualRows.size());
        final long fillsBefore = ColumnChunkPageStore.sparseFillCount();
        final long hitsBefore = ColumnChunkPageStore.sparseHitCount();
        try (final WritableChunk<Values> expectedChunk = expected.getChunkType().makeWritableChunk(step);
                final WritableChunk<Values> actualChunk = actual.getChunkType().makeWritableChunk(step);
                final RowSequence.Iterator expectedIt = expectedRows.getRowSequenceIterator();
                final RowSequence.Iterator it = actualRows.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final RowSequence stepRows = it.getNextRowSequenceWithLength(step);
                try (final ChunkSource.FillContext expectedContext = expected.makeFillContext(step);
                        final ChunkSource.FillContext actualContext = actual.makeFillContext(step)) {
                    expected.fillChunk(expectedContext, expectedChunk, expectedIt.getNextRowSequenceWithLength(step));
                    actual.fillChunk(actualContext, actualChunk, stepRows);
                }
                for (int ii = 0; ii < expectedChunk.size(); ++ii) {
                    final Object expectedValue = ChunkBoxer.boxedGet(expectedChunk, ii);
                    final Object actualValue = ChunkBoxer.boxedGet(actualChunk, ii);
                    assertTrue(ii + ": " + expectedValue + " != " + actualValue,
                            Objects.deepEquals(expectedValue, actualValue));
                }
            }
        }
        return new long[] {ColumnChunkPageStore.sparseFillCount() - fillsBefore,
                ColumnChunkPageStore.sparseHitCount() - hitsBefore};
    }

    /**
     * Assert pass {@code ii} reads {@code filters[ii]} from {@code source} as {@code reads[ii]} says.
     */
    private static void assertPasses(final String path, final Table source, final String[] filters,
            final Reads[] reads) {
        for (int ii = 0; ii < filters.length; ++ii) {
            final String filter = filters[ii];
            assertPass(filter, dense(() -> ParquetTools.readTable(path).where(filter).select()), source.where(filter),
                    reads[ii]);
        }
    }

    /**
     * Readers on separate threads merge into and read from the same pages' cached values.
     */
    @Test
    public void testConcurrentReadersMatchDense() throws Exception {
        final String path = writeTestTable();
        assertConcurrentReadersMatchDense(dense(() -> ParquetTools.readTable(path).select()),
                ParquetTools.readTable(path)).close();
    }

    /**
     * Readers' own rows are sparse and their shared rows dense, so pages are cached whole and their sparse values
     * dropped while other readers publish to them.
     */
    @Test
    public void testConcurrentDenseAndSparseReadersMatchDense() throws Exception {
        ColumnChunkPageStore.setSparseReadMaxDensity(0.125);
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final long before = ColumnChunkPageStore.sparseFillCount();
        assertConcurrentReadersMatchDense(expected, ParquetTools.readTable(path)).close();
        assertTrue("the readers' own rows should read sparsely", ColumnChunkPageStore.sparseFillCount() > before);
    }

    /**
     * Concurrent readers promote pages while others publish to them. Afterward, every page is cached either whole or
     * with all the rows read, so another pass decodes nothing.
     */
    @Test
    public void testConcurrentReadersPromote() throws Exception {
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(2);
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final Table actual = ParquetTools.readTable(path);
        try (final SafeCloseable ignored = PageCache.pinCreatedPages();
                final RowSet allRows = assertConcurrentReadersMatchDense(expected, actual)) {
            final long before = ColumnChunkPageStore.sparseFillCount();
            for (final String column : expected.getDefinition().getColumnNamesArray()) {
                assertFillsMatch(column, expected.getColumnSource(column), actual.getColumnSource(column), allRows,
                        false);
            }
            assertEquals("a pass after the readers should decode nothing", before,
                    ColumnChunkPageStore.sparseFillCount());
        }
    }

    /**
     * Readers whose contexts carry hints publish every hinted row of each page they open, while other readers read and
     * publish to the same pages. Even readers hint every reader's rows; odd readers hint only the next reader's, so
     * they also read rows outside their hint.
     */
    @Test
    public void testConcurrentHintedReadersMatchDense() throws Exception {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final long before = ColumnChunkPageStore.sparseFillCount();
        assertConcurrentReadersMatchDense(expected, ParquetTools.readTable(path), true).close();
        assertTrue("the readers should read sparsely", ColumnChunkPageStore.sparseFillCount() > before);
    }

    private static RowSet assertConcurrentReadersMatchDense(final Table expected, final Table actual)
            throws Exception {
        return assertConcurrentReadersMatchDense(expected, actual, false);
    }

    /**
     * @param hinted Whether the readers' contexts carry hints; see {@link #testConcurrentHintedReadersMatchDense()}
     * @return The union of the readers' rows, which the caller must close
     */
    private static RowSet assertConcurrentReadersMatchDense(final Table expected, final Table actual,
            final boolean hinted) throws Exception {
        final int numThreads = 4;
        final List<RowSet> rows = new ArrayList<>();
        for (int tt = 0; tt < numThreads; ++tt) {
            rows.add(expected.where("ii % 17 == " + tt).getRowSet().copy());
        }
        final RowSet allRows = expected.where("ii % 17 < " + numThreads).getRowSet().copy();
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        try {
            final List<Future<?>> futures = new ArrayList<>();
            for (int tt = 0; tt < numThreads; ++tt) {
                final RowSet threadRows = rows.get(tt);
                final RowSet hint = !hinted ? null : tt % 2 == 0 ? allRows : rows.get((tt + 1) % numThreads);
                futures.add(executor.submit(() -> {
                    for (int rep = 0; rep < 3; ++rep) {
                        for (final String column : expected.getDefinition().getColumnNamesArray()) {
                            assertFillsMatch(column, expected.getColumnSource(column), actual.getColumnSource(column),
                                    threadRows, false, hint);
                            assertFillsMatch(column, expected.getColumnSource(column), actual.getColumnSource(column),
                                    allRows, false, hint);
                        }
                    }
                }));
            }
            for (final Future<?> future : futures) {
                future.get();
            }
        } catch (final Exception | Error e) {
            allRows.close();
            throw e;
        } finally {
            executor.shutdownNow();
            rows.forEach(RowSet::close);
        }
        return allRows;
    }

    private enum Reads {
        /** Some fills decode. */
        DECODE,
        /** No fill decodes, and some are served from cached values. */
        HIT,
        /** No fill decodes or uses cached values: the pages are cached whole. */
        DENSE,
        /** Only the result is checked. */
        ANY
    }

    private static void assertPass(final String name, final Table expected, final Table filtered, final Reads reads) {
        final long fillsBefore = ColumnChunkPageStore.sparseFillCount();
        final long hitsBefore = ColumnChunkPageStore.sparseHitCount();
        assertTableEquals(expected, filtered);
        final long fills = ColumnChunkPageStore.sparseFillCount() - fillsBefore;
        final long hits = ColumnChunkPageStore.sparseHitCount() - hitsBefore;
        final String counts = name + ": " + fills + " sparse fills, " + hits + " hits";
        switch (reads) {
            case DECODE:
                assertTrue(counts, fills > 0);
                break;
            case HIT:
                assertTrue(counts, fills == 0 && hits > 0);
                break;
            case DENSE:
                assertTrue(counts, fills == 0 && hits == 0);
                break;
            case ANY:
                break;
        }
    }

    /**
     * A sort's redirection fills the source once per output chunk, each fill scattered over every page.
     */
    @Test
    public void testRedirectedReadsMatchDense() {
        final String path = writeTestTable();
        final String hash = "H_ = (ii * 2654435761L) % 1000003L";
        final Table expected = dense(() -> ParquetTools.readTable(path).updateView(hash).sort("H_").select());
        final long before = ColumnChunkPageStore.sparseFillCount();
        final Table actual = ParquetTools.readTable(path).updateView(hash).sort("H_");
        assertTableEquals(expected, actual);
        assertTrue("the first chunk should read sparsely", ColumnChunkPageStore.sparseFillCount() > before);
    }

    /**
     * A sort over a static parent hints its parent's rows, so a viewport scrolling the sort decodes each page once, as
     * a single pass in file order would, rather than once per step that touches it.
     */
    @Test
    public void testRowsOfInterestDecodeEachPageOnce() {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages();
                final RowSet filteredRows = expected.where(FILTERS[0]).getRowSet().copy()) {
            final long pages = fillSteps(expected.getColumnSource("L"),
                    ParquetTools.readTable(path).getColumnSource("L"), filteredRows, filteredRows.intSize())[0];
            final long[] hinted = scrollSorted(path, expected, FILTERS[0], true);
            final long[] unhinted = scrollSorted(path, expected, FILTERS[0], false);
            assertEquals("hinted sparse fills", pages, hinted[0]);
            assertTrue("unhinted sparse fills", unhinted[0] > pages);
        }
    }

    /**
     * A natural join over a static right table hints the right table's rows, so a viewport scrolling the join in an
     * order scattered over the right table decodes each right page once.
     */
    @Test
    public void testJoinRowsOfInterestDecodeEachPageOnce() {
        final String path = writeTestTable();
        final String rightFilter = FILTERS[0] + " && !IsNull";
        final String hash = "H_ = (ii * 2654435761L) % 1000003L";
        final Table expected = dense(() -> ParquetTools.readTable(path).where(rightFilter).updateView(hash)
                .sort("H_").view("K = L").naturalJoin(ParquetTools.readTable(path).where(rightFilter), "K=L", "D")
                .select());
        final Table denseAll = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final Table right = ParquetTools.readTable(path).where(rightFilter);
            final long pages = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), right.getRowSet(), right.intSize())[0];
            final Table joined = ParquetTools.readTable(path).where(rightFilter).updateView(hash).sort("H_")
                    .view("K = L").naturalJoin(right, "K=L", "D");
            final long[] reads = fillSteps(expected.getColumnSource("D"), joined.getColumnSource("D"),
                    joined.getRowSet(), 50);
            assertEquals("sparse fills", pages, reads[0]);
        }
    }

    /**
     * A natural join of static tables hints the right rows it matched, not the right table's rows: an unfiltered right
     * table would hint every row, which is dense enough to cache every page whole. A viewport scrolling the join
     * decodes each matched right page once instead.
     */
    @Test
    public void testJoinHintsMatchedRows() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseReadMaxDensity(0.05);
        final String leftFilter = "ii % 97 == 0 && !IsNull";
        final String hash = "H_ = (ii * 2654435761L) % 1000003L";
        final Table expected = dense(() -> ParquetTools.readTable(path).where(leftFilter).updateView(hash)
                .sort("H_").view("K = L").naturalJoin(ParquetTools.readTable(path).updateView("RK = ii"), "K=RK", "D")
                .select());
        final Table denseAll = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages();
                final RowSet matched = denseAll.where(leftFilter).getRowSet().copy()) {
            final long pages = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), matched, matched.intSize())[0];
            final Table joined = ParquetTools.readTable(path).where(leftFilter).updateView(hash).sort("H_")
                    .view("K = L").naturalJoin(ParquetTools.readTable(path).updateView("RK = ii"), "K=RK", "D");
            final long[] reads = fillSteps(expected.getColumnSource("D"), joined.getColumnSource("D"),
                    joined.getRowSet(), 50);
            assertTrue("should read sparsely", pages > 0);
            assertEquals("sparse fills", pages, reads[0]);
        }
    }

    /**
     * A first-by or last-by of a static table hints the table's rows, so a viewport scrolling the result in an order
     * scattered over the table caches each page whole on its first read, rather than opening it sparsely again and
     * again.
     */
    @Test
    public void testFirstAndLastByCachePagesWhole() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseReadMaxDensity(0.05);
        final String group = "G = ii - ii % 97";
        final String hash = "H_ = (G * 2654435761L) % 1000003L";
        for (final boolean first : new boolean[] {true, false}) {
            final Function<Table, Table> agg = table -> {
                final Table grouped = table.updateView(group);
                return (first ? grouped.firstBy("G") : grouped.lastBy("G")).updateView(hash).sort("H_");
            };
            final Table expected = dense(() -> agg.apply(ParquetTools.readTable(path)).select());
            try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
                final long opensBefore = ColumnChunkPageStore.sparseOpenCount();
                final Table actual = agg.apply(ParquetTools.readTable(path));
                final long[] reads = fillSteps(expected.getColumnSource("D"), actual.getColumnSource("D"),
                        actual.getRowSet(), 50);
                final String label = first ? "first" : "last";
                assertEquals(label + " sparse opens", 0, ColumnChunkPageStore.sparseOpenCount() - opensBefore);
                assertEquals(label + " sparse fills", 0, reads[0]);
            }
        }
    }

    /**
     * An as-of join over a static right table hints the right rows it matched, so a viewport scrolling the join in an
     * order scattered over the right table decodes each right page once.
     */
    @Test
    public void testAsOfJoinRowsOfInterestDecodeEachPageOnce() {
        assertJoinDecodesEachPageOnce((left, right) -> left.aj(right, "K>=L", "D"));
    }

    /**
     * A cross join over a static right table hints the right table's rows, so a viewport scrolling the join in an order
     * scattered over the right table decodes each right page once.
     */
    @Test
    public void testCrossJoinRowsOfInterestDecodeEachPageOnce() {
        assertJoinDecodesEachPageOnce((left, right) -> left.join(right, "K=L", "D"));
    }

    /**
     * Join a left table, ordered by a hash and with one row per right row, to the right table's {@code D}, and assert
     * that scrolling the result decodes each right page once.
     */
    private void assertJoinDecodesEachPageOnce(final BinaryOperator<Table> join) {
        final String path = writeTestTable();
        final String rightFilter = FILTERS[0] + " && !IsNull";
        final String hash = "H_ = (ii * 2654435761L) % 1000003L";
        final Table expected = dense(() -> join.apply(
                ParquetTools.readTable(path).where(rightFilter).updateView(hash).sort("H_").view("K = L"),
                ParquetTools.readTable(path).where(rightFilter)).select());
        final Table denseAll = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final Table right = ParquetTools.readTable(path).where(rightFilter);
            final long pages = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), right.getRowSet(), right.intSize())[0];
            final Table joined = join.apply(
                    ParquetTools.readTable(path).where(rightFilter).updateView(hash).sort("H_").view("K = L"), right);
            final long[] reads = fillSteps(expected.getColumnSource("D"), expected.getRowSet(),
                    joined.getColumnSource("D"), joined.getRowSet(), 50);
            assertTrue("should read sparsely", pages > 0);
            assertEquals("sparse fills", pages, reads[0]);
        }
    }

    /**
     * Only sources that may read a page sparsely read hints, so hints are made only for them: not for columns read
     * while sparse reads are disabled, repeated columns, or sources outside Parquet.
     */
    @Test
    public void testHintsOnlyForSourcesThatReadThem() {
        final String path = writeTestTable();
        final Table table = ParquetTools.readTable(path);
        assertTrue(RowsOfInterest.readBy(table.getColumnSource("L")));
        assertTrue(RowsOfInterest.readBy(table.getColumnSource("Sym")));
        // Through its native source, as a snapshot reads it.
        assertTrue(RowsOfInterest.readBy(table.getColumnSource("Inst")));
        assertTrue(RowsOfInterest.readBy(ReinterpretUtils.maybeConvertToPrimitive(table.getColumnSource("Inst"))));
        assertNotNull(RowsOfInterest.of(table, table.getColumnSource("L")));
        // Repeated.
        assertFalse(RowsOfInterest.readBy(table.getColumnSource("Arr")));
        assertNull(RowsOfInterest.of(table, table.getColumnSource("Arr")));
        // In memory.
        final Table selected = dense(() -> ParquetTools.readTable(path).select());
        assertFalse(RowsOfInterest.readBy(selected.getColumnSource("L")));
        // Read while sparse reads were disabled; enabling them later does not add hints. Coalescing reads the regions.
        final Table disabled = dense(() -> ParquetTools.readTable(path).coalesce());
        assertFalse(RowsOfInterest.readBy(disabled.getColumnSource("L")));
        assertNull(RowsOfInterest.of(disabled, disabled.getColumnSource("L")));
    }

    /**
     * A join whose right table was read while sparse reads were disabled makes no hint, so scrolling it once they are
     * enabled reads as an unhinted join does, decoding some right pages more than once.
     */
    @Test
    public void testNoJoinHintForSourcesReadWhileSparseReadsDisabled() {
        final String path = writeTestTable();
        final String rightFilter = FILTERS[0] + " && !IsNull";
        final String hash = "H_ = (ii * 2654435761L) % 1000003L";
        final Table expected = dense(() -> ParquetTools.readTable(path).where(rightFilter).updateView(hash)
                .sort("H_").view("K = L").naturalJoin(ParquetTools.readTable(path).where(rightFilter), "K=L", "D")
                .select());
        final Table denseAll = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final Table right = dense(() -> ParquetTools.readTable(path).where(rightFilter));
            final long pages = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), right.getRowSet(), right.intSize())[0];
            final Table joined = ParquetTools.readTable(path).where(rightFilter).updateView(hash).sort("H_")
                    .view("K = L").naturalJoin(right, "K=L", "D");
            final long[] reads = fillSteps(expected.getColumnSource("D"), joined.getColumnSource("D"),
                    joined.getRowSet(), 50);
            assertTrue("unhinted sparse fills: " + reads[0] + ", pages: " + pages, reads[0] > pages);
        }
    }

    /**
     * A hint supplied lazily, as a join's is, is computed only when a page is opened sparsely: not when a context is
     * made, nor on reads with sparse reads disabled or served from pages cached whole.
     */
    @Test
    public void testLazyHintComputedOnlyOnSparseOpen() {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final Table sorted = ParquetTools.readTable(path).updateView("H_ = (ii * 2654435761L) % 1000003L").sort("H_");
        final RowRedirection redirection = SortOperation.getRowRedirection(sorted);
        final ColumnSource<?> expectedSource =
                RedirectedColumnSource.maybeRedirect(redirection, expected.getColumnSource("L"));
        final AtomicInteger calls = new AtomicInteger();
        final Supplier<RowSet> hint = () -> {
            calls.incrementAndGet();
            return expected.getRowSet();
        };
        try (final SafeCloseable ignored = PageCache.pinCreatedPages();
                final RowSet rows = sorted.getRowSet().subSetByPositionRange(0, 50)) {
            final ColumnSource<?> actual = RedirectedColumnSource.alwaysRedirect(redirection,
                    ParquetTools.readTable(path).getColumnSource("L"), hint);
            try (final ChunkSource.FillContext ignored2 = actual.makeFillContext(50)) {
                assertEquals("making a context", 0, calls.get());
            }
            dense(() -> fillSteps(expectedSource, actual, rows, 50));
            assertEquals("sparse reads disabled", 0, calls.get());
            final long[] cached = fillSteps(expectedSource, actual, rows, 50);
            assertEquals("pages cached whole: sparse fills", 0, cached[0]);
            assertEquals("pages cached whole", 0, calls.get());
            final ColumnSource<?> fresh = RedirectedColumnSource.alwaysRedirect(redirection,
                    ParquetTools.readTable(path).getColumnSource("L"), hint);
            assertTrue("should read sparsely", fillSteps(expectedSource, fresh, rows, 50)[0] > 0);
            assertEquals("one context, which opened pages sparsely", 1, calls.get());
        }
    }

    /**
     * A lazy hint estimated to cover most of a page caches the page whole without being computed; one estimated sparse
     * is computed as usual.
     */
    @Test
    public void testDenseEstimateSkipsHint() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseReadMaxDensity(0.05);
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final Table sorted = ParquetTools.readTable(path).updateView("H_ = (ii * 2654435761L) % 1000003L").sort("H_");
        final RowRedirection redirection = SortOperation.getRowRedirection(sorted);
        final ColumnSource<?> expectedSource =
                RedirectedColumnSource.maybeRedirect(redirection, expected.getColumnSource("L"));
        final AtomicInteger calls = new AtomicInteger();
        try (final SafeCloseable ignored = PageCache.pinCreatedPages();
                final RowSet requested = sorted.getRowSet().subSetByPositionRange(0, 50);
                final RowSet none = RowSetFactory.empty()) {
            // Advisory: an empty hint leaves the sparse reads to the requests.
            final Supplier<RowSet> rows = () -> {
                calls.incrementAndGet();
                return none;
            };
            final ColumnSource<?> denseHinted = RedirectedColumnSource.alwaysRedirect(redirection,
                    ParquetTools.readTable(path).getColumnSource("L"), estimatedHint(rows, 1));
            assertEquals("dense estimate: sparse fills", 0, fillSteps(expectedSource, denseHinted, requested, 50)[0]);
            assertEquals("dense estimate", 0, calls.get());
            final ColumnSource<?> sparseHinted = RedirectedColumnSource.alwaysRedirect(redirection,
                    ParquetTools.readTable(path).getColumnSource("L"), estimatedHint(rows, 0));
            assertTrue("should read sparsely", fillSteps(expectedSource, sparseHinted, requested, 50)[0] > 0);
            assertEquals("sparse estimate", 1, calls.get());
        }
    }

    /**
     * @return A hint of {@code rows}, estimated to cover {@code density} of any range
     */
    private static RowsOfInterest.Estimated estimatedHint(final Supplier<RowSet> rows, final double density) {
        return new RowsOfInterest.Estimated() {
            @Override
            public RowSet get() {
                return rows.get();
            }

            @Override
            public double estimateRowsIn(final long firstKey, final long lastKey) {
                return density * (lastKey - firstKey + 1);
            }
        };
    }

    /**
     * A hint is advisory. After a hinted open decodes the hinted rows of a page, a later fill on the same context of an
     * unhinted row behind the cursor caches the page whole, and one past the cursor resumes it.
     */
    @Test
    public void testReadsOutsideHintMatchDense() {
        final String path = writeTestTable();
        final ColumnSource<?> expected = dense(() -> ParquetTools.readTable(path).select()).getColumnSource("L");
        try (final SafeCloseable ignored = PageCache.pinCreatedPages();
                final RowSet hint = RowSetFactory.fromKeys(10, 20, 30);
                final RowSet hinted = RowSetFactory.fromKeys(10);
                final RowSet cachedRow = RowSetFactory.fromKeys(20);
                final RowSet behind = RowSetFactory.fromKeys(15);
                final RowSet behindAgain = RowSetFactory.fromKeys(16);
                final RowSet after = RowSetFactory.fromKeys(40)) {
            final ColumnSource<?> behindSource = ParquetTools.readTable(path).getColumnSource("L");
            final long[][] behindReads = fillSequence(expected, behindSource, hint, hinted, behind);
            assertArrayEquals("hinted open", new long[] {1, 1, 0}, behindReads[0]);
            assertArrayEquals("behind the cursor: cached whole", new long[] {0, 0, 0}, behindReads[1]);
            assertArrayEquals("served by the whole page", new long[] {0, 0, 0},
                    fillSequence(expected, behindSource, null, behindAgain)[0]);

            final long[][] afterReads =
                    fillSequence(expected, ParquetTools.readTable(path).getColumnSource("L"), hint, hinted, cachedRow,
                            after);
            assertArrayEquals("hinted open", new long[] {1, 1, 0}, afterReads[0]);
            assertArrayEquals("hinted row: cached", new long[] {0, 0, 1}, afterReads[1]);
            assertArrayEquals("past the cursor: resumed", new long[] {1, 0, 0}, afterReads[2]);
        }
    }

    /**
     * A hint that holds only some of the rows read, through a sort's redirection, reads what a dense read does, for
     * every column.
     */
    @Test
    public void testPartialHintMatchesDense() {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final Table actual = ParquetTools.readTable(path);
        final Table sorted = actual.updateView("H_ = (ii * 2654435761L) % 1000003L").sort("H_");
        final RowRedirection redirection = SortOperation.getRowRedirection(sorted);
        final long before = ColumnChunkPageStore.sparseFillCount();
        // Not every row: with every page read sparsely, each step opens most pages.
        try (final RowSet hint = expected.where("ii % 3 == 0").getRowSet().copy();
                final RowSet rows = sorted.getRowSet().subSetByPositionRange(0, 2000)) {
            for (final String column : expected.getDefinition().getColumnNamesArray()) {
                fillSteps(RedirectedColumnSource.maybeRedirect(redirection, expected.getColumnSource(column)),
                        RedirectedColumnSource.maybeRedirect(redirection, actual.getColumnSource(column), hint),
                        rows, 50);
            }
        }
        assertTrue("should read sparsely", ColumnChunkPageStore.sparseFillCount() > before);
    }

    /**
     * Fill {@code steps} in turn on one context of {@code actual}, hinted {@code hint}, checking each against
     * {@code expected}.
     *
     * @return For each step, its sparse fills, opens, and hits
     */
    private static long[][] fillSequence(final ColumnSource<?> expected, final ColumnSource<?> actual,
            @Nullable final RowSet hint, final RowSet... steps) {
        final long[][] counts = new long[steps.length][];
        final int capacity = Arrays.stream(steps).mapToInt(RowSet::intSize).max().orElse(1);
        try (final SharedContext sharedContext = hint == null ? null : hinted(hint);
                final ChunkSource.FillContext expectedContext = expected.makeFillContext(capacity);
                final ChunkSource.FillContext actualContext = actual.makeFillContext(capacity, sharedContext);
                final WritableChunk<Values> expectedChunk = expected.getChunkType().makeWritableChunk(capacity);
                final WritableChunk<Values> actualChunk = actual.getChunkType().makeWritableChunk(capacity)) {
            for (int ss = 0; ss < steps.length; ++ss) {
                final long fills = ColumnChunkPageStore.sparseFillCount();
                final long opens = ColumnChunkPageStore.sparseOpenCount();
                final long hits = ColumnChunkPageStore.sparseHitCount();
                expected.fillChunk(expectedContext, expectedChunk, steps[ss]);
                actual.fillChunk(actualContext, actualChunk, steps[ss]);
                counts[ss] = new long[] {ColumnChunkPageStore.sparseFillCount() - fills,
                        ColumnChunkPageStore.sparseOpenCount() - opens, ColumnChunkPageStore.sparseHitCount() - hits};
                assertEquals(expectedChunk.size(), actualChunk.size());
                for (int ii = 0; ii < expectedChunk.size(); ++ii) {
                    assertEquals("step " + ss, (Object) ChunkBoxer.boxedGet(expectedChunk, ii),
                            ChunkBoxer.boxedGet(actualChunk, ii));
                }
            }
        }
        return counts;
    }

    /**
     * @return A shared context carrying {@code hint}, which the caller must close
     */
    private static SharedContext hinted(final RowSet hint) {
        final SharedContext sharedContext = SharedContext.makeSharedContext();
        RowsOfInterest.attach(sharedContext, hint);
        return sharedContext;
    }

    /**
     * A group-by over a static table hints the table's rows to the contexts its vectors make, so reading every group,
     * each scattered over every page, decodes each page once.
     */
    @Test
    public void testGroupByRowsOfInterestDecodeEachPageOnce() {
        final String path = writeTestTable();
        final String group = "G = (ii * 2654435761L) % 97";
        final Table expected =
                dense(() -> ParquetTools.readTable(path).where(FILTERS[0]).updateView(group).groupBy("G").select());
        final Table denseAll = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final Table filtered = ParquetTools.readTable(path).where(FILTERS[0]);
            final long pages = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), filtered.getRowSet(), filtered.intSize())[0];
            final Table grouped = filtered.updateView(group).groupBy("G");
            final ColumnSource<?> expectedSource = expected.getColumnSource("D");
            final ColumnSource<?> actualSource = grouped.getColumnSource("D");
            final long before = ColumnChunkPageStore.sparseFillCount();
            try (final RowSet.Iterator it = grouped.getRowSet().iterator()) {
                while (it.hasNext()) {
                    final long key = it.nextLong();
                    final Object expectedValues = ((Vector<?>) expectedSource.get(key)).toArray();
                    final Object actualValues = ((Vector<?>) actualSource.get(key)).toArray();
                    assertTrue("group " + key, Objects.deepEquals(expectedValues, actualValues));
                }
            }
            assertEquals("sparse fills", pages, ColumnChunkPageStore.sparseFillCount() - before);
        }
    }

    /**
     * A snapshot of a static table hints the table's rows, so viewport snapshots stepping through a filtered table in
     * file order decode each page once, rather than again on each page a step continues onto.
     */
    @Test
    public void testSnapshotRowsOfInterestDecodeEachPageOnce() {
        final String path = writeTestTable();
        final Table denseAll = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final Table filtered = ParquetTools.readTable(path).where(FILTERS[0]).view("D");
            final long pages = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), filtered.getRowSet(), filtered.intSize())[0];
            final long unhinted = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), filtered.getRowSet(), 50)[0];
            final long before = ColumnChunkPageStore.sparseFillCount();
            for (long first = 0; first < filtered.size(); first += 50) {
                final long last = Math.min(first + 50, filtered.size()) - 1;
                try (final RowSequence positions = RowSequenceFactory.forRange(first, last);
                        final BarrageMessage snapshot = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                                this, (BaseTable<?>) filtered, null, positions, null)) {
                    assertEquals("rows", last - first + 1, snapshot.rowsIncluded.size());
                }
            }
            assertTrue("unhinted steps should decode some pages more than once", unhinted > pages);
            assertEquals("sparse fills", pages, ColumnChunkPageStore.sparseFillCount() - before);
        }
    }

    /**
     * As {@link #testSnapshotRowsOfInterestDecodeEachPageOnce()}, over runs of rows at the default cutoff: a step
     * within a run, or one ending just past a page's start, looks dense to the fill, but the hint shows the page is
     * sparse.
     */
    @Test
    public void testSnapshotRunsDecodeEachPageOnce() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseReadMaxDensity(0.125);
        final Table denseAll = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final Table filtered = ParquetTools.readTable(path).where("ii % 5000 < 64").view("D");
            final long pages = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), filtered.getRowSet(), filtered.intSize())[0];
            final long before = ColumnChunkPageStore.sparseFillCount();
            for (long first = 0; first < filtered.size(); first += 50) {
                final long last = Math.min(first + 50, filtered.size()) - 1;
                try (final RowSequence positions = RowSequenceFactory.forRange(first, last);
                        final BarrageMessage snapshot = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                                this, (BaseTable<?>) filtered, null, positions, null)) {
                    assertEquals("rows", last - first + 1, snapshot.rowsIncluded.size());
                }
            }
            assertEquals("sparse fills", pages, ColumnChunkPageStore.sparseFillCount() - before);
        }
    }

    /**
     * The sort's hint reaches the page store through a reverse and a view, so viewport snapshots of the result decode
     * each page once, and scrolling back over them hits the cache.
     */
    @Test
    public void testReversedViewSnapshotsDecodeEachPageOnce() {
        final String path = writeTestTable();
        final String hash = "H_ = (ii * 2654435761L) % 1000003L";
        final Table denseAll = dense(() -> ParquetTools.readTable(path).select());
        final Table expected = denseAll.where(FILTERS[0]).updateView(hash).sort("H_").reverse().view("L");
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final Table filtered = ParquetTools.readTable(path).where(FILTERS[0]);
            final long pages = fillSteps(denseAll.getColumnSource("L"),
                    ParquetTools.readTable(path).getColumnSource("L"), filtered.getRowSet(), filtered.intSize())[0];
            final Table viewed = filtered.updateView(hash).sort("H_").reverse().view("L");
            final long before = ColumnChunkPageStore.sparseFillCount();
            for (long first = 0; first < viewed.size(); first += 50) {
                final long last = Math.min(first + 50, viewed.size()) - 1;
                try (final RowSequence positions = RowSequenceFactory.forRange(first, last);
                        final BarrageMessage snapshot = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                                this, (BaseTable<?>) viewed, null, positions, null)) {
                    assertEquals("rows", last - first + 1, snapshot.rowsIncluded.size());
                }
            }
            assertEquals("sparse fills", pages, ColumnChunkPageStore.sparseFillCount() - before);
            final long[] rescroll = fillSteps(expected.getColumnSource("L"), expected.getRowSet(),
                    viewed.getColumnSource("L"), viewed.getRowSet(), 50);
            assertEquals("rescroll sparse fills", 0, rescroll[0]);
        }
    }

    /**
     * A formula over a static table hints the table's rows to its inputs, so viewport steps through a filtered table in
     * file order decode each page once, with no snapshot to supply the hint.
     */
    @Test
    public void testFormulaRowsOfInterestDecodeEachPageOnce() {
        final String path = writeTestTable();
        final Table denseAll = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final Table filtered = ParquetTools.readTable(path).where(FILTERS[0]);
            final long pages = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), filtered.getRowSet(), filtered.intSize())[0];
            final long unhinted = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), filtered.getRowSet(), 50)[0];
            final Table expected = denseAll.updateView("D_f = D * 2");
            final Table formula = filtered.updateView("D_f = D * 2").view("D_f");
            final long before = ColumnChunkPageStore.sparseFillCount();
            fillSteps(expected.getColumnSource("D_f"), formula.getColumnSource("D_f"), filtered.getRowSet(), 50);
            assertTrue("unhinted steps should decode some pages more than once", unhinted > pages);
            assertEquals("sparse fills", pages, ColumnChunkPageStore.sparseFillCount() - before);
        }
    }

    /**
     * As {@link #testFormulaRowsOfInterestDecodeEachPageOnce()}, over runs of rows: a step within a run makes a
     * contiguous {@code getChunk}, which must be served from the page's sparse values rather than materialize it.
     */
    @Test
    public void testFormulaContiguousStepsDecodeEachPageOnce() {
        final String path = writeTestTable();
        final Table denseAll = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final Table filtered = ParquetTools.readTable(path).where("ii % 5000 < 64");
            final long pages = fillSteps(denseAll.getColumnSource("D"),
                    ParquetTools.readTable(path).getColumnSource("D"), filtered.getRowSet(), filtered.intSize())[0];
            final Table expected = denseAll.updateView("D_f = D * 2");
            final Table formula = filtered.updateView("D_f = D * 2").view("D_f");
            final long before = ColumnChunkPageStore.sparseFillCount();
            fillSteps(expected.getColumnSource("D_f"), formula.getColumnSource("D_f"), filtered.getRowSet(), 50);
            assertEquals("sparse fills", pages, ColumnChunkPageStore.sparseFillCount() - before);
        }
    }

    /**
     * Only formulas read hinted inputs: a passthrough column is the parent's source itself.
     */
    @Test
    public void testPassthroughColumnsAreNotWrapped() {
        final Table filtered = ParquetTools.readTable(writeTestTable()).where(FILTERS[0]);
        final ColumnSource<?> source = filtered.getColumnSource("D");
        assertTrue(RowsOfInterest.readBy(source));
        assertSame(source, filtered.view("D", "D_f = D * 2").getColumnSource("D"));
        assertSame(source, filtered.updateView("D_f = D * 2").getColumnSource("D"));
    }

    /**
     * A hint covering more of a page than the density cutoff caches the page whole on its first read, as a read without
     * sparse support would, rather than opening it sparsely first.
     */
    @Test
    public void testRowsOfInterestAboveCutoffCachesPagesWhole() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseReadMaxDensity(0.05);
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        try (final SafeCloseable ignored = PageCache.pinCreatedPages()) {
            final long opensBefore = ColumnChunkPageStore.sparseOpenCount();
            final long[] hinted = scrollSorted(path, expected, "true", true);
            assertEquals("sparse opens", 0, ColumnChunkPageStore.sparseOpenCount() - opensBefore);
            assertEquals("sparse fills", 0, hinted[0]);
            final long[] unhinted = scrollSorted(path, expected, "true", false);
            assertTrue("unhinted should read sparsely", unhinted[0] > 0);
        }
    }

    /**
     * Scroll column {@code L} of a fresh read of {@code path}, filtered and then sorted by a hash, in steps of 50, with
     * or without the sort's hint, and check it against {@code expected}.
     *
     * @return The sparse fills and hits
     */
    private static long[] scrollSorted(final String path, final Table expected, final String filter,
            final boolean hinted) {
        final Table sorted = ParquetTools.readTable(path).where(filter)
                .updateView("H_ = (ii * 2654435761L) % 1000003L").sort("H_");
        final RowRedirection redirection = SortOperation.getRowRedirection(sorted);
        final ColumnSource<?> actual = hinted ? sorted.getColumnSource("L")
                : RedirectedColumnSource.maybeRedirect(redirection,
                        ParquetTools.readTable(path).getColumnSource("L"));
        final ColumnSource<?> expectedSource =
                RedirectedColumnSource.maybeRedirect(redirection, expected.getColumnSource("L"));
        return fillSteps(expectedSource, actual, sorted.getRowSet(), 50);
    }

    private static void assertFillsMatch(
            final String column,
            final ColumnSource<?> expected,
            final ColumnSource<?> actual,
            final RowSet rows,
            final boolean reverse) {
        assertFillsMatch(column, expected, actual, rows, reverse, null);
    }

    /**
     * @param hint If not {@code null}, hinted to {@code actual} through its fill context's shared context
     */
    private static void assertFillsMatch(
            final String column,
            final ColumnSource<?> expected,
            final ColumnSource<?> actual,
            final RowSet rows,
            final boolean reverse,
            @Nullable final RowSet hint) {
        final int chunkSize = 37;
        final List<RowSet> chunks = new ArrayList<>();
        try (final RowSequence.Iterator it = rows.getRowSequenceIterator()) {
            while (it.hasMore()) {
                chunks.add(it.getNextRowSequenceWithLength(chunkSize).asRowSet().copy());
            }
        }
        if (reverse) {
            Collections.reverse(chunks);
        }
        try (final SharedContext sharedContext = hint == null ? null : hinted(hint);
                final ChunkSource.FillContext expectedContext = expected.makeFillContext(chunkSize);
                final ChunkSource.FillContext actualContext = actual.makeFillContext(chunkSize, sharedContext);
                final WritableChunk<Values> expectedChunk = expected.getChunkType().makeWritableChunk(chunkSize);
                final WritableChunk<Values> actualChunk = actual.getChunkType().makeWritableChunk(chunkSize)) {
            for (final RowSet chunk : chunks) {
                expected.fillChunk(expectedContext, expectedChunk, chunk);
                actual.fillChunk(actualContext, actualChunk, chunk);
                assertEquals(column, expectedChunk.size(), actualChunk.size());
                for (int ii = 0; ii < expectedChunk.size(); ++ii) {
                    final Object expectedValue = ChunkBoxer.boxedGet(expectedChunk, ii);
                    final Object actualValue = ChunkBoxer.boxedGet(actualChunk, ii);
                    assertTrue(column + "[" + ii + "]: " + expectedValue + " != " + actualValue,
                            Objects.deepEquals(expectedValue, actualValue));
                }
                chunk.close();
            }
        }
    }

    private static void assertSparseMatchesDense(final String path, final boolean expectSparse) {
        for (final String filter : FILTERS) {
            final Table expected = dense(() -> ParquetTools.readTable(path).where(filter).select());
            final long before = ColumnChunkPageStore.sparseFillCount();
            // A fresh read, so that no page is already cached.
            final Table actual = ParquetTools.readTable(path).where(filter);
            assertTableEquals(expected, actual);
            if (expectSparse && expected.size() > 0) {
                assertTrue(path + " where " + filter + " should read sparsely",
                        ColumnChunkPageStore.sparseFillCount() > before);
            }
        }
    }

    /**
     * Run {@code read} with sparse reads disabled. The setting is process-wide, so {@code read} must realize its result
     * (with {@code select}) before returning: a deferred table would be read later, sparsely.
     */
    private static <T> T dense(final java.util.function.Supplier<T> read) {
        final double maxDensity = ColumnChunkPageStore.setSparseReadMaxDensity(0);
        try {
            return read.get();
        } finally {
            ColumnChunkPageStore.setSparseReadMaxDensity(maxDensity);
        }
    }

    private static String resource(final String name) {
        return SparsePageReadTest.class.getResource(name).getFile();
    }
}
