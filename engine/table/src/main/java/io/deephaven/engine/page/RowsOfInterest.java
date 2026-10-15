//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.page;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ResettableContext;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

/**
 * A hint, carried by a {@link SharedContext}, of every row key a consumer may read from the sources it passes that
 * context to. A paging source may decode them all on its first read of a page, rather than only the rows requested, or
 * cache the page whole if they are most of it. The hint is advisory: a superset is safe, and a row outside it is read
 * as usual.
 * <p>
 * The row keys are those of the sources the context is passed to. A source that reads its inner sources at other row
 * keys, as a redirection does, must give them a context of their own.
 */
public final class RowsOfInterest implements ResettableContext {

    private static final SharedContext.Key<RowsOfInterest> KEY = new SharedContext.Key<>() {};
    /**
     * Set by the first {@link #attach}, which may follow a {@link #get}: {@link SharedContext} has no lookup that does
     * not insert.
     */
    @Nullable
    private Supplier<RowSet> rows;

    private RowsOfInterest() {}

    /**
     * @return Whether {@code source} may read a hint from the shared contexts it is given; see
     *         {@link PagingChunkSource#readsRowsOfInterest()}
     */
    public static boolean readBy(@NotNull final ColumnSource<?> source) {
        return source instanceof PagingChunkSource && ((PagingChunkSource<?>) source).readsRowsOfInterest();
    }

    /**
     * @return The rows of {@code table}, as a hint for reads of its column sources, or {@code null} if it is
     *         refreshing: its rows may change while they are read
     */
    @Nullable
    public static RowSet of(@NotNull final Table table) {
        return table.isRefreshing() ? null : table.getRowSet();
    }

    /**
     * @return As {@link #of(Table)}, for reads of {@code source}, one of {@code table}'s column sources; or
     *         {@code null} if {@code source} would not read it
     */
    @Nullable
    public static RowSet of(@NotNull final Table table, @NotNull final ColumnSource<?> source) {
        return readBy(source) ? of(table) : null;
    }

    /**
     * Attach {@code rows} to {@code sharedContext}, unless it already carries a hint.
     *
     * @param rows The row keys of interest, or {@code null} for none; not owned, and must not change while
     *        {@code sharedContext} is in use
     */
    public static void attach(@NotNull final SharedContext sharedContext, @Nullable final RowSet rows) {
        if (rows != null) {
            attach(sharedContext, () -> rows);
        }
    }

    /**
     * As {@link #attach(SharedContext, RowSet)}, with the rows computed when a source first reads them.
     *
     * @param rows Supplies the row keys of interest, or {@code null} for none; called on each read of the hint, so
     *        should be cheap after the first call
     */
    public static void attach(@NotNull final SharedContext sharedContext, @Nullable final Supplier<RowSet> rows) {
        if (rows != null) {
            final RowsOfInterest hint = sharedContext.getOrCreate(KEY, RowsOfInterest::new);
            if (hint.rows == null) {
                hint.rows = rows;
            }
        }
    }

    /**
     * @return The hint of {@code sharedContext}, or {@code null} if it is {@code null}; rows attached later are seen by
     *         {@link #rows()}
     */
    @Nullable
    public static RowsOfInterest get(@Nullable final SharedContext sharedContext) {
        return sharedContext == null ? null : sharedContext.getOrCreate(KEY, RowsOfInterest::new);
    }

    /**
     * @return The row keys of interest, or {@code null} if none are attached yet; may compute them, so call only to use
     *         them
     */
    @Nullable
    public RowSet rows() {
        return rows == null ? null : rows.get();
    }

    /**
     * @return An estimate of how many row keys of interest lie in {@code [firstKey, lastKey]}, made without computing
     *         them, or {@code -1} if there is none: the hint is not {@link Estimated}
     */
    public double estimateRowsIn(final long firstKey, final long lastKey) {
        return rows instanceof Estimated ? ((Estimated) rows).estimateRowsIn(firstKey, lastKey) : -1;
    }

    /**
     * Supplies rows of interest that are costly to compute, and estimates them cheaply, so that a source which would
     * read a page whole anyway need not compute them.
     */
    public interface Estimated extends Supplier<RowSet> {
        /**
         * @return An estimate of how many of the rows {@link #get()} supplies lie in {@code [firstKey, lastKey]}
         */
        double estimateRowsIn(long firstKey, long lastKey);
    }

    @Override
    public void reset() {}
}
