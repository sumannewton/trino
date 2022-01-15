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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.jmh.Benchmarks;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.GroupByIdBlock;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.UnscaledDecimal128Arithmetic;
import io.trino.sql.tree.QualifiedName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDecimalAggregation
{
    private static final int ELEMENT_COUNT = 10_000;

    @Benchmark
    @OperationsPerInvocation(ELEMENT_COUNT)
    public GroupedAccumulator benchmark(BenchmarkData data)
    {
        GroupedAccumulator accumulator = data.getAccumulator();
        accumulator.addInput(data.getGroupIds(), data.getValues());
        return accumulator;
    }

    @Benchmark
    public Block benchmarkEvaluateIntermediate(BenchmarkData data)
    {
        GroupedAccumulator accumulator = data.getAccumulatorFactory().createGroupedAccumulator();
        accumulator.addInput(data.getGroupIds(), data.getValues());
        BlockBuilder builder = accumulator.getIntermediateType().createBlockBuilder(null, data.getGroupCount());
        for (int groupId = 0; groupId < data.getGroupCount(); groupId++) {
            accumulator.evaluateIntermediate(groupId, builder);
        }
        return builder.build();
    }

    @Benchmark
    public Block benchmarkEvaluateFinal(BenchmarkData data)
    {
        GroupedAccumulator accumulator = data.getAccumulatorFactory().createGroupedIntermediateAccumulator();
        // Add the intermediate input multiple times to invoke the combine behavior
        accumulator.addIntermediate(data.getGroupIds(), data.getIntermediateValues());
        accumulator.addIntermediate(data.getGroupIds(), data.getIntermediateValues());
        BlockBuilder builder = accumulator.getFinalType().createBlockBuilder(null, data.getGroupCount());
        for (int groupId = 0; groupId < data.getGroupCount(); groupId++) {
            accumulator.evaluateFinal(groupId, builder);
        }
        return builder.build();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"SHORT", "LONG"})
        private String type = "SHORT";

        @Param({"avg", "sum"})
        private String function = "avg";

        @Param({"10", "1000"})
        private int groupCount = 10;

        private AccumulatorFactory factory;
        private GroupedAccumulator accumulator;
        private GroupByIdBlock groupIds;
        private Page values;
        private Block intermediateValues;

        @Setup
        public void setup()
        {
            Metadata metadata = createTestMetadataManager();

            switch (type) {
                case "SHORT": {
                    DecimalType type = createDecimalType(14, 3);
                    values = createValues(metadata, type, type::writeLong);
                    break;
                }
                case "LONG": {
                    DecimalType type = createDecimalType(30, 10);
                    values = createValues(metadata, type, (builder, value) -> type.writeSlice(builder, UnscaledDecimal128Arithmetic.unscaledDecimal(value)));
                    break;
                }
            }

            BlockBuilder ids = BIGINT.createBlockBuilder(null, ELEMENT_COUNT);
            for (int i = 0; i < ELEMENT_COUNT; i++) {
                BIGINT.writeLong(ids, ThreadLocalRandom.current().nextLong(groupCount));
            }
            groupIds = new GroupByIdBlock(groupCount, ids.build());
            intermediateValues = createIntermediateValues(factory.createGroupedAccumulator(), groupIds, values);
        }

        private Block createIntermediateValues(GroupedAccumulator accumulator, GroupByIdBlock groupIds, Page inputPage)
        {
            accumulator.addInput(groupIds, inputPage);
            BlockBuilder builder = accumulator.getIntermediateType().createBlockBuilder(null, toIntExact(groupIds.getGroupCount()));
            for (int groupId = 0; groupId < groupIds.getGroupCount(); groupId++) {
                accumulator.evaluateIntermediate(groupId, builder);
            }
            return builder.build();
        }

        private Page createValues(Metadata metadata, DecimalType type, ValueWriter writer)
        {
            ResolvedFunction resolvedFunction = metadata.resolveFunction(TEST_SESSION, QualifiedName.of(function), fromTypes(type));
            InternalAggregationFunction implementation = metadata.getAggregateFunctionImplementation(resolvedFunction);
            factory = implementation.bind(ImmutableList.of(0), Optional.empty());
            accumulator = factory.createGroupedAccumulator();

            BlockBuilder builder = type.createBlockBuilder(null, ELEMENT_COUNT);
            for (int i = 0; i < ELEMENT_COUNT; i++) {
                writer.write(builder, i);
            }
            return new Page(builder.build());
        }

        public AccumulatorFactory getAccumulatorFactory()
        {
            return factory;
        }

        public GroupedAccumulator getAccumulator()
        {
            return accumulator;
        }

        public Page getValues()
        {
            return values;
        }

        public GroupByIdBlock getGroupIds()
        {
            return groupIds;
        }

        public int getGroupCount()
        {
            return groupCount;
        }

        public Block getIntermediateValues()
        {
            return intermediateValues;
        }

        interface ValueWriter
        {
            void write(BlockBuilder valuesBuilder, int value);
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();

        assertEquals(data.groupIds.getPositionCount(), data.getValues().getPositionCount());

        new BenchmarkDecimalAggregation().benchmark(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // ensure the benchmarks are valid before running
        new BenchmarkDecimalAggregation().verify();

        Benchmarks.benchmark(BenchmarkDecimalAggregation.class, WarmupMode.BULK).run();
    }
}
