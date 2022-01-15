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

import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionXxHash64;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.wrappedLongArray;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createArrayBigintBlock;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createLongDecimalsBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createShortDecimalsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.operator.aggregation.ChecksumAggregationFunction.PRIME64;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.util.Arrays.asList;

public class TestChecksumAggregation
{
    private static final Metadata metadata = createTestMetadataManager();
    private static final BlockTypeOperators blockTypeOperators = new BlockTypeOperators();

    @Test
    public void testEmpty()
    {
        ResolvedFunction booleanAgg = metadata.resolveFunction(TEST_SESSION, QualifiedName.of("checksum"), fromTypes(BOOLEAN));
        assertAggregation(metadata, booleanAgg, null, createBooleansBlock());
    }

    @Test
    public void testBoolean()
    {
        ResolvedFunction booleanAgg = metadata.resolveFunction(TEST_SESSION, QualifiedName.of("checksum"), fromTypes(BOOLEAN));
        Block block = createBooleansBlock(null, null, true, false, false);
        assertAggregation(metadata, booleanAgg, expectedChecksum(BOOLEAN, block), block);
    }

    @Test
    public void testLong()
    {
        ResolvedFunction longAgg = metadata.resolveFunction(TEST_SESSION, QualifiedName.of("checksum"), fromTypes(BIGINT));
        Block block = createLongsBlock(null, 1L, 2L, 100L, null, Long.MAX_VALUE, Long.MIN_VALUE);
        assertAggregation(metadata, longAgg, expectedChecksum(BIGINT, block), block);
    }

    @Test
    public void testDouble()
    {
        ResolvedFunction doubleAgg = metadata.resolveFunction(TEST_SESSION, QualifiedName.of("checksum"), fromTypes(DOUBLE));
        Block block = createDoublesBlock(null, 2.0, null, 3.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN);
        assertAggregation(metadata, doubleAgg, expectedChecksum(DOUBLE, block), block);
    }

    @Test
    public void testString()
    {
        ResolvedFunction stringAgg = metadata.resolveFunction(TEST_SESSION, QualifiedName.of("checksum"), fromTypes(VARCHAR));
        Block block = createStringsBlock("a", "a", null, "b", "c");
        assertAggregation(metadata, stringAgg, expectedChecksum(VARCHAR, block), block);
    }

    @Test
    public void testShortDecimal()
    {
        ResolvedFunction decimalAgg = metadata.resolveFunction(TEST_SESSION, QualifiedName.of("checksum"), fromTypes(createDecimalType(10, 2)));
        Block block = createShortDecimalsBlock("11.11", "22.22", null, "33.33", "44.44");
        DecimalType shortDecimalType = createDecimalType(1);
        assertAggregation(metadata, decimalAgg, expectedChecksum(shortDecimalType, block), block);
    }

    @Test
    public void testLongDecimal()
    {
        ResolvedFunction decimalAgg = metadata.resolveFunction(TEST_SESSION, QualifiedName.of("checksum"), fromTypes(createDecimalType(19, 2)));
        Block block = createLongDecimalsBlock("11.11", "22.22", null, "33.33", "44.44");
        DecimalType longDecimalType = createDecimalType(19);
        assertAggregation(metadata, decimalAgg, expectedChecksum(longDecimalType, block), block);
    }

    @Test
    public void testArray()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        ResolvedFunction stringAgg = metadata.resolveFunction(TEST_SESSION, QualifiedName.of("checksum"), fromTypes(arrayType));
        Block block = createArrayBigintBlock(asList(null, asList(1L, 2L), asList(3L, 4L), asList(5L, 6L)));
        assertAggregation(metadata, stringAgg, expectedChecksum(arrayType, block), block);
    }

    private static SqlVarbinary expectedChecksum(Type type, Block block)
    {
        BlockPositionXxHash64 xxHash64Operator = blockTypeOperators.getXxHash64Operator(type);
        long result = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                result += PRIME64;
            }
            else {
                result += xxHash64Operator.xxHash64(block, i) * PRIME64;
            }
        }
        return new SqlVarbinary(wrappedLongArray(result).getBytes());
    }
}
