package io.deephaven.db.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.BooleanUtils;
import io.deephaven.db.util.tuples.generated.ByteLongDoubleTuple;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ByteChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.DoubleChunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.tuples.AbstractTupleSource;
import io.deephaven.db.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Byte, DBDateTime, and Double.
 * <p>Generated by {@link io.deephaven.db.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ReinterpretedBooleanDateTimeDoubleColumnTupleSource extends AbstractTupleSource<ByteLongDoubleTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ReinterpretedBooleanDateTimeDoubleColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ByteLongDoubleTuple, Byte, DBDateTime, Double> FACTORY = new Factory();

    private final ColumnSource<Byte> columnSource1;
    private final ColumnSource<DBDateTime> columnSource2;
    private final ColumnSource<Double> columnSource3;

    public ReinterpretedBooleanDateTimeDoubleColumnTupleSource(
            @NotNull final ColumnSource<Byte> columnSource1,
            @NotNull final ColumnSource<DBDateTime> columnSource2,
            @NotNull final ColumnSource<Double> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ByteLongDoubleTuple createTuple(final long indexKey) {
        return new ByteLongDoubleTuple(
                columnSource1.getByte(indexKey),
                DBTimeUtils.nanos(columnSource2.get(indexKey)),
                columnSource3.getDouble(indexKey)
        );
    }

    @Override
    public final ByteLongDoubleTuple createPreviousTuple(final long indexKey) {
        return new ByteLongDoubleTuple(
                columnSource1.getPrevByte(indexKey),
                DBTimeUtils.nanos(columnSource2.getPrev(indexKey)),
                columnSource3.getPrevDouble(indexKey)
        );
    }

    @Override
    public final ByteLongDoubleTuple createTupleFromValues(@NotNull final Object... values) {
        return new ByteLongDoubleTuple(
                BooleanUtils.booleanAsByte((Boolean)values[0]),
                DBTimeUtils.nanos((DBDateTime)values[1]),
                TypeUtils.unbox((Double)values[2])
        );
    }

    @Override
    public final ByteLongDoubleTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ByteLongDoubleTuple(
                TypeUtils.unbox((Byte)values[0]),
                DBTimeUtils.nanos((DBDateTime)values[1]),
                TypeUtils.unbox((Double)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ByteLongDoubleTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getFirstElement()));
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DBTimeUtils.nanosToTime(tuple.getSecondElement()));
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final ByteLongDoubleTuple tuple) {
        return new SmartKey(
                BooleanUtils.byteAsBoolean(tuple.getFirstElement()),
                DBTimeUtils.nanosToTime(tuple.getSecondElement()),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final ByteLongDoubleTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return BooleanUtils.byteAsBoolean(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DBTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ByteLongDoubleTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DBTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<ByteLongDoubleTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ByteChunk<Attributes.Values> chunk1 = chunks[0].asByteChunk();
        ObjectChunk<DBDateTime, Attributes.Values> chunk2 = chunks[1].asObjectChunk();
        DoubleChunk<Attributes.Values> chunk3 = chunks[2].asDoubleChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ByteLongDoubleTuple(chunk1.get(ii), DBTimeUtils.nanos(chunk2.get(ii)), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ReinterpretedBooleanDateTimeDoubleColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ByteLongDoubleTuple, Byte, DBDateTime, Double> {

        private Factory() {
        }

        @Override
        public TupleSource<ByteLongDoubleTuple> create(
                @NotNull final ColumnSource<Byte> columnSource1,
                @NotNull final ColumnSource<DBDateTime> columnSource2,
                @NotNull final ColumnSource<Double> columnSource3
        ) {
            return new ReinterpretedBooleanDateTimeDoubleColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
