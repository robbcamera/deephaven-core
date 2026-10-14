//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.page;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.SharedContext;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class RowsOfInterestTest {

    @Test
    public void testNoSharedContext() {
        assertNull(RowsOfInterest.get(null));
    }

    /**
     * A source may look for a hint before another source sharing its context attaches one.
     */
    @Test
    public void testAttachAfterGet() {
        try (final SharedContext sharedContext = SharedContext.makeSharedContext();
                final RowSet rows = RowSetFactory.fromKeys(1, 5)) {
            final RowsOfInterest hint = RowsOfInterest.get(sharedContext);
            // noinspection DataFlowIssue: not null for a shared context
            assertNull(hint.rows());
            RowsOfInterest.attach(sharedContext, rows);
            assertSame(rows, hint.rows());
            assertSame(hint, RowsOfInterest.get(sharedContext));
        }
    }

    @Test
    public void testFirstAttachWins() {
        try (final SharedContext sharedContext = SharedContext.makeSharedContext();
                final RowSet first = RowSetFactory.fromKeys(1);
                final RowSet second = RowSetFactory.fromKeys(2)) {
            RowsOfInterest.attach(sharedContext, (RowSet) null);
            RowsOfInterest.attach(sharedContext, first);
            RowsOfInterest.attach(sharedContext, () -> second);
            // noinspection DataFlowIssue: not null for a shared context
            assertSame(first, RowsOfInterest.get(sharedContext).rows());
        }
    }
}
