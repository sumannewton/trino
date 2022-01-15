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
package io.trino.operator.output;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PartitionedOutputBuffer;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.OperatorContext;
import io.trino.operator.PartitionFunction;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongDictionaryBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createRLEBlock;
import static io.trino.execution.buffer.BufferState.OPEN;
import static io.trino.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

public class TestPartitionedOutputOperator
{
    private static final DataSize MAX_MEMORY = DataSize.of(50, MEGABYTE);
    private static final DataSize PARTITION_MAX_MEMORY = DataSize.of(5, MEGABYTE);

    private static final int PAGE_COUNT = 10;
    private static final int POSITIONS_PER_PAGE = 1000;
    private static final int PARTITION_COUNT = 512;

    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final List<Type> REPLICATION_TYPES = ImmutableList.of(BIGINT, BIGINT);

    private static final Block NULL_BLOCK = new RunLengthEncodedBlock(BIGINT.createBlockBuilder(null, 1).appendNull().build(), POSITIONS_PER_PAGE);
    private static final Block TESTING_BLOCK = createLongSequenceBlock(0, POSITIONS_PER_PAGE);
    private static final Block TESTING_DICTIONARY_BLOCK = createLongDictionaryBlock(0, POSITIONS_PER_PAGE);
    private static final Block TESTING_RLE_BLOCK = createRLEBlock(new Random(0).nextLong(), POSITIONS_PER_PAGE);
    private static final Page TESTING_PAGE = new Page(TESTING_BLOCK);
    private static final Page TESTING_PAGE_WITH_NULL_BLOCK = new Page(POSITIONS_PER_PAGE, NULL_BLOCK, TESTING_BLOCK);

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(1, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    public void testOutputForSimplePage()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(false);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(TESTING_PAGE);
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getPositionCount());
    }

    @Test
    public void testOutputForPageWithDictionary()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(false);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(new Page(TESTING_DICTIONARY_BLOCK));
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getPositionCount());
    }

    @Test
    public void testOutputForPageWithRunLength()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(false);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(new Page(TESTING_RLE_BLOCK));
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getPositionCount());
    }

    @Test
    public void testOutputForSimplePageAndReplication()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(true);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(new Page(POSITIONS_PER_PAGE, NULL_BLOCK, TESTING_BLOCK));
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getPositionCount());
    }

    @Test
    public void testOutputForPageWithDictionaryAndReplication()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(true);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(new Page(POSITIONS_PER_PAGE, NULL_BLOCK, TESTING_DICTIONARY_BLOCK));
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getPositionCount());
    }

    @Test
    public void testOutputForPageWithRunLengthAndReplication()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(true);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(new Page(POSITIONS_PER_PAGE, NULL_BLOCK, TESTING_RLE_BLOCK));
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getPositionCount());
    }

    private PartitionedOutputOperator createPartitionedOutputOperator(boolean shouldReplicate)
    {
        BlockTypeOperators blockTypeOperators = new BlockTypeOperators(new TypeOperators());
        PartitionFunction partitionFunction = new LocalPartitionGenerator(
                new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0}, blockTypeOperators),
                PARTITION_COUNT);
        PagesSerdeFactory serdeFactory = new PagesSerdeFactory(createTestMetadataManager().getBlockEncodingSerde(), false);

        DriverContext driverContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(MAX_MEMORY)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OutputBuffers buffers = OutputBuffers.createInitialEmptyOutputBuffers(PARTITIONED);
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
        }
        PartitionedOutputBuffer buffer = new PartitionedOutputBuffer(
                "task-instance-id",
                new StateMachine<>("bufferState", scheduledExecutor, OPEN, TERMINAL_BUFFER_STATES),
                buffers.withNoMoreBufferIds(),
                DataSize.ofBytes(Long.MAX_VALUE),
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                scheduledExecutor);

        PartitionedOutputOperator.PartitionedOutputFactory operatorFactory;
        if (shouldReplicate) {
            operatorFactory = new PartitionedOutputOperator.PartitionedOutputFactory(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty()),
                    true,
                    OptionalInt.of(0),
                    buffer,
                    PARTITION_MAX_MEMORY);
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), REPLICATION_TYPES, Function.identity(), serdeFactory)
                    .createOperator(driverContext);
        }
        else {
            operatorFactory = new PartitionedOutputOperator.PartitionedOutputFactory(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty(), Optional.empty()),
                    false,
                    OptionalInt.empty(),
                    buffer,
                    PARTITION_MAX_MEMORY);
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), TYPES, Function.identity(), serdeFactory)
                    .createOperator(driverContext);
        }
    }
}
