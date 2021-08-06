package io.deephaven.db.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.util.tuples.generated.IntByteObjectTuple;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ByteChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.tuples.AbstractTupleSource;
import io.deephaven.db.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Integer, Byte, and Object.
 * <p>Generated by {@link io.deephaven.db.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class IntegerByteObjectColumnTupleSource extends AbstractTupleSource<IntByteObjectTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link IntegerByteObjectColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<IntByteObjectTuple, Integer, Byte, Object> FACTORY = new Factory();

    private final ColumnSource<Integer> columnSource1;
    private final ColumnSource<Byte> columnSource2;
    private final ColumnSource<Object> columnSource3;

    public IntegerByteObjectColumnTupleSource(
            @NotNull final ColumnSource<Integer> columnSource1,
            @NotNull final ColumnSource<Byte> columnSource2,
            @NotNull final ColumnSource<Object> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final IntByteObjectTuple createTuple(final long indexKey) {
        return new IntByteObjectTuple(
                columnSource1.getInt(indexKey),
                columnSource2.getByte(indexKey),
                columnSource3.get(indexKey)
        );
    }

    @Override
    public final IntByteObjectTuple createPreviousTuple(final long indexKey) {
        return new IntByteObjectTuple(
                columnSource1.getPrevInt(indexKey),
                columnSource2.getPrevByte(indexKey),
                columnSource3.getPrev(indexKey)
        );
    }

    @Override
    public final IntByteObjectTuple createTupleFromValues(@NotNull final Object... values) {
        return new IntByteObjectTuple(
                TypeUtils.unbox((Integer)values[0]),
                TypeUtils.unbox((Byte)values[1]),
                values[2]
        );
    }

    @Override
    public final IntByteObjectTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new IntByteObjectTuple(
                TypeUtils.unbox((Integer)values[0]),
                TypeUtils.unbox((Byte)values[1]),
                values[2]
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final IntByteObjectTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final IntByteObjectTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                tuple.getThirdElement()
        );
    }

    @Override
    public final Object exportElement(@NotNull final IntByteObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final IntByteObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<IntByteObjectTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        IntChunk<Attributes.Values> chunk1 = chunks[0].asIntChunk();
        ByteChunk<Attributes.Values> chunk2 = chunks[1].asByteChunk();
        ObjectChunk<Object, Attributes.Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new IntByteObjectTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link IntegerByteObjectColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<IntByteObjectTuple, Integer, Byte, Object> {

        private Factory() {
        }

        @Override
        public TupleSource<IntByteObjectTuple> create(
                @NotNull final ColumnSource<Integer> columnSource1,
                @NotNull final ColumnSource<Byte> columnSource2,
                @NotNull final ColumnSource<Object> columnSource3
        ) {
            return new IntegerByteObjectColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
