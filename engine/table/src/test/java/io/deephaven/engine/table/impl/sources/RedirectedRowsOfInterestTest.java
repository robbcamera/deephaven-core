//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.page.RowsOfInterest;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.util.function.Supplier;

import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class RedirectedRowsOfInterestTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * Spans several of the computation's chunks, maps them out of order, maps many left rows to each right row, and
     * leaves some unmapped.
     */
    @Test
    public void testMatchedRows() {
        final Table left = TableTools.emptyTable(10_000);
        final Table right = TableTools.emptyTable(20_000);
        final WritableRowRedirection redirection = WritableRowRedirection.FACTORY.createRowRedirection(10_000);
        final RowSetBuilderRandom expected = RowSetFactory.builderRandom();
        for (long key = 0; key < left.size(); ++key) {
            if (key % 5 != 0) {
                final long rightKey = (key * 7919) % 5000;
                redirection.put(key, rightKey);
                expected.addKey(rightKey);
            }
        }
        final Supplier<RowSet> hint = RedirectedRowsOfInterest.of(left, right, redirection);
        try (final RowSet expectedRows = expected.build()) {
            assertTrue(expectedRows.size() < 5000);
            assertEquals(expectedRows, hint.get());
        }
        assertSame("computed once", hint.get(), hint.get());
    }

    /**
     * Before the rows are computed, a range's estimate is its share of the right rows scaled by the left table's size;
     * after, it is exact.
     */
    @Test
    public void testEstimateRowsIn() {
        final Table left = TableTools.emptyTable(10);
        final Table right = TableTools.emptyTable(100).where("ii % 2 == 0");
        final WritableRowRedirection redirection = WritableRowRedirection.FACTORY.createRowRedirection(10);
        for (long key = 0; key < left.size(); ++key) {
            redirection.put(key, 2 * key);
        }
        final RowsOfInterest.Estimated hint =
                (RowsOfInterest.Estimated) RedirectedRowsOfInterest.of(left, right, redirection);
        // 10 left rows over 50 right rows; [0, 19] holds 10 right rows, [1, 1] none.
        assertEquals(2.0, hint.estimateRowsIn(0, 19), 0);
        assertEquals(0.0, hint.estimateRowsIn(1, 1), 0);
        assertEquals(10.0, hint.estimateRowsIn(0, 99), 0);
        hint.get();
        assertEquals(10.0, hint.estimateRowsIn(0, 19), 0);
        assertEquals(0.0, hint.estimateRowsIn(20, 99), 0);
    }

    /**
     * The rows are computed on the first read of the hint, not when it is made: a join makes it for every right column,
     * read or not.
     */
    @Test
    public void testComputedOnFirstGet() {
        final Table left = TableTools.emptyTable(2);
        final Table right = TableTools.emptyTable(100);
        final WritableRowRedirection redirection = WritableRowRedirection.FACTORY.createRowRedirection(2);
        redirection.put(0, 10);
        final Supplier<RowSet> hint = RedirectedRowsOfInterest.of(left, right, redirection);
        redirection.put(1, 20);
        try (final RowSet expected = RowSetFactory.fromKeys(10, 20)) {
            assertEquals(expected, hint.get());
            redirection.put(1, 30);
            assertEquals(expected, hint.get());
        }
    }

    @Test
    public void testLargerLeftHintsRightRows() {
        final Table left = TableTools.emptyTable(300);
        final Table right = TableTools.emptyTable(200);
        final WritableRowRedirection redirection = WritableRowRedirection.FACTORY.createRowRedirection(300);
        redirection.put(0, 10);
        assertSame(right.getRowSet(), RedirectedRowsOfInterest.of(left, right, redirection).get());
    }

    @Test
    public void testRefreshingLeftHintsRightRows() {
        final Table left = testRefreshingTable(intCol("A", 1, 2));
        final Table right = TableTools.emptyTable(200);
        final WritableRowRedirection redirection = WritableRowRedirection.FACTORY.createRowRedirection(2);
        redirection.put(0, 10);
        assertSame(right.getRowSet(), RedirectedRowsOfInterest.of(left, right, redirection).get());
    }

    @Test
    public void testRefreshingRightHintsNothing() {
        final Table left = TableTools.emptyTable(2);
        final Table right = testRefreshingTable(intCol("A", 1, 2, 3));
        final WritableRowRedirection redirection = WritableRowRedirection.FACTORY.createRowRedirection(2);
        assertNull(RedirectedRowsOfInterest.of(left, right, redirection));
    }
}
