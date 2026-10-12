//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.page;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ResettableContext;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A hint, carried by a {@link SharedContext}, of every row key a consumer may read from the sources it passes that
 * context to. A paging source may decode them all on its first read of a page, rather than only the rows requested. The
 * hint is advisory: a superset is safe, and a row outside it is read as usual.
 */
public final class RowsOfInterest implements ResettableContext {

    private static final SharedContext.Key<RowsOfInterest> KEY = new SharedContext.Key<>() {};
    private static final RowsOfInterest NONE = new RowsOfInterest(null);

    @Nullable
    private final RowSet rows;

    private RowsOfInterest(@Nullable final RowSet rows) {
        this.rows = rows;
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
     * Attach {@code rows} to {@code sharedContext}, unless it already carries a hint.
     *
     * @param rows The row keys of interest, or {@code null} for none; not owned, and must not change while
     *        {@code sharedContext} is in use
     */
    public static void attach(@NotNull final SharedContext sharedContext, @Nullable final RowSet rows) {
        if (rows != null) {
            sharedContext.getOrCreate(KEY, () -> new RowsOfInterest(rows));
        }
    }

    /**
     * @return The row keys of interest attached to {@code sharedContext}, or {@code null} if none
     */
    @Nullable
    public static RowSet get(@Nullable final SharedContext sharedContext) {
        return sharedContext == null ? null : sharedContext.getOrCreate(KEY, () -> NONE).rows;
    }

    @Override
    public void reset() {}
}
