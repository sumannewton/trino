/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.aggregation.minmaxby;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.AbstractRowBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public abstract class KeyAndBlockPositionValueStateSerializer<T extends KeyAndBlockPositionValueState>
        implements AccumulatorStateSerializer<T>
{
    final Type firstType;
    protected final Type secondType;

    abstract void readFirstField(Block block, int index, T state);

    abstract void writeFirstField(BlockBuilder out, T state);

    KeyAndBlockPositionValueStateSerializer(Type firstType, Type secondType)
    {
        this.firstType = requireNonNull(firstType, "firstType is null");
        this.secondType = requireNonNull(secondType, "secondType is null");
    }

    @Override
    public Type getSerializedType()
    {
        // Order must match StateCompiler.generateStateSerializer, which is used for min_max_by with primitive arguments
        // Types are:: field order is: first, firstNull, second, secondNull
        return RowType.anonymous(ImmutableList.of(firstType, BOOLEAN, secondType, BOOLEAN));
    }

    @Override
    public void serialize(T state, BlockBuilder out)
    {
        BlockBuilder blockBuilder = out.beginBlockEntry();
        if (state.isFirstNull()) {
            blockBuilder.appendNull();
        }
        else {
            writeFirstField(blockBuilder, state);
        }
        BOOLEAN.writeBoolean(blockBuilder, state.isFirstNull());

        if (state.isSecondNull()) {
            blockBuilder.appendNull();
        }
        else {
            secondType.appendTo(state.getSecondBlock(), state.getSecondPosition(), blockBuilder);
        }
        BOOLEAN.writeBoolean(blockBuilder, state.isSecondNull());
        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, T state)
    {
        checkArgument(block instanceof AbstractRowBlock);
        ColumnarRow columnarRow = toColumnarRow(block);

        state.setFirstNull(BOOLEAN.getBoolean(columnarRow.getField(1), index));
        state.setSecondNull(BOOLEAN.getBoolean(columnarRow.getField(3), index));

        if (!state.isFirstNull()) {
            readFirstField(columnarRow.getField(0), index, state);
        }

        if (!state.isSecondNull()) {
            state.setSecondPosition(index);
            state.setSecondBlock(columnarRow.getField(2));
        }
    }
}
