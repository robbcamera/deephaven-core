//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.page.RowsOfInterest;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Passes {@link RowsOfInterest} to every context it makes for its delegate, for consumers that read a source through
 * contexts of their own making, such as group-by vectors and formula inputs.
 */
public final class RowsOfInterestColumnSource<T> extends DelegatingColumnSource<T, T> {

    /**
     * @return {@code source}, wrapped to hint the rows of {@code table} if it is static and {@code source} reads hints
     */
    public static <T> ColumnSource<T> maybeWrap(@NotNull final ColumnSource<T> source, @NotNull final Table table) {
        // Only paging sources read the hint, and they keep nothing else in their shared context: another source may
        // cache per-chunk state there, which a context made here would never reset.
        final RowSet rowsOfInterest = RowsOfInterest.of(table, source);
        if (rowsOfInterest == null) {
            return source;
        }
        return new RowsOfInterestColumnSource<>(source, rowsOfInterest);
    }

    private final Supplier<RowSet> rowsOfInterest;

    private RowsOfInterestColumnSource(@NotNull final ColumnSource<T> source, @NotNull final RowSet rowsOfInterest) {
        super(source.getType(), source.getComponentType(), source);
        this.rowsOfInterest = () -> rowsOfInterest;
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return delegate.makeFillContext(chunkCapacity, hint());
    }

    @Override
    public GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return delegate.makeGetContext(chunkCapacity, hint());
    }

    /**
     * @return A shared context holding only the hint. The caller's is not used: skipping a shared context is always
     *         safe, and it may already hold another hint. It is never closed: the delegate keeps nothing in it, and the
     *         hint holds no resources.
     */
    private SharedContext hint() {
        final SharedContext hint = SharedContext.makeSharedContext();
        RowsOfInterest.attach(hint, rowsOfInterest);
        return hint;
    }
}
