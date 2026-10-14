//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.page.RowsOfInterest;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

/**
 * The {@link RowsOfInterest} of a join's redirected right columns: the right rows its redirection maps the left rows
 * to, computed when a source first reads the hint. A join reads only those, which may be far fewer than the right
 * table's rows.
 */
public final class RedirectedRowsOfInterest implements Supplier<RowSet> {

    private static final int CHUNK_SIZE = 4096;

    /**
     * @return The hint for reads of {@code rightTable}'s sources through {@code rowRedirection}: the rows it maps
     *         {@code leftTable}'s rows to if both are static and {@code leftTable} is no larger, else
     *         {@code rightTable}'s rows if it is static, else {@code null}
     */
    @Nullable
    public static Supplier<RowSet> of(
            @NotNull final Table leftTable,
            @NotNull final Table rightTable,
            @NotNull final RowRedirection rowRedirection) {
        // noinspection resource: the table owns its rows
        final RowSet rightRows = RowsOfInterest.of(rightTable);
        if (rightRows == null) {
            return null;
        }
        // A refreshing left table may change the redirection; hint every row it may map to. A larger one would cost
        // more to map than the right rows it could rule out.
        if (leftTable.isRefreshing() || leftTable.size() > rightRows.size()) {
            return () -> rightRows;
        }
        return new RedirectedRowsOfInterest(leftTable.getRowSet(), rowRedirection);
    }

    @Nullable
    private RowSet leftRows;
    private final RowRedirection rowRedirection;
    private volatile RowSet rows;

    private RedirectedRowsOfInterest(@NotNull final RowSet leftRows, @NotNull final RowRedirection rowRedirection) {
        this.leftRows = leftRows;
        this.rowRedirection = rowRedirection;
    }

    @Override
    public RowSet get() {
        RowSet result = rows;
        if (result == null) {
            synchronized (this) {
                result = rows;
                if (result == null) {
                    // noinspection DataFlowIssue: cleared only once rows is set
                    rows = result = redirectedRows(leftRows, rowRedirection);
                    leftRows = null;
                }
            }
        }
        return result;
    }

    private static RowSet redirectedRows(@NotNull final RowSet leftRows, @NotNull final RowRedirection rowRedirection) {
        // Sorted a chunk at a time, so that memory is bounded by the result rather than by leftRows.
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        try (final ChunkSource.FillContext fillContext = rowRedirection.makeFillContext(CHUNK_SIZE, null);
                final WritableLongChunk<RowKeys> chunk = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
                final RowSequence.Iterator it = leftRows.getRowSequenceIterator()) {
            while (it.hasMore()) {
                rowRedirection.fillChunk(fillContext, chunk, it.getNextRowSequenceWithLength(CHUNK_SIZE));
                chunk.sort();
                long previous = RowSequence.NULL_ROW_KEY;
                for (int ii = 0; ii < chunk.size(); ++ii) {
                    final long key = chunk.get(ii);
                    if (key != previous) {
                        builder.addKey(key);
                        previous = key;
                    }
                }
            }
        }
        return builder.build();
    }
}
