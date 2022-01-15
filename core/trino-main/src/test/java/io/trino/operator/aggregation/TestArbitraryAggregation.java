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
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Arrays;

import static io.trino.block.BlockAssertions.createArrayBigintBlock;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertNotNull;

public class TestArbitraryAggregation
{
    private static final Metadata metadata = createTestMetadataManager();

    @Test
    public void testAllRegistered()
    {
        for (Type valueType : metadata.getTypes()) {
            assertNotNull(metadata.getAggregateFunctionImplementation(metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(valueType))));
        }
    }

    @Test
    public void testNullBoolean()
    {
        ResolvedFunction booleanAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(BOOLEAN));
        assertAggregation(
                metadata,
                booleanAgg,
                null,
                createBooleansBlock((Boolean) null));
    }

    @Test
    public void testValidBoolean()
    {
        ResolvedFunction booleanAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(BOOLEAN));
        assertAggregation(
                metadata,
                booleanAgg,
                true,
                createBooleansBlock(true, true));
    }

    @Test
    public void testNullLong()
    {
        ResolvedFunction longAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(BIGINT));
        assertAggregation(
                metadata,
                longAgg,
                null,
                createLongsBlock(null, null));
    }

    @Test
    public void testValidLong()
    {
        ResolvedFunction longAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(BIGINT));
        assertAggregation(
                metadata,
                longAgg,
                1L,
                createLongsBlock(1L, null));
    }

    @Test
    public void testNullDouble()
    {
        ResolvedFunction doubleAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(DOUBLE));
        assertAggregation(
                metadata,
                doubleAgg,
                null,
                createDoublesBlock(null, null));
    }

    @Test
    public void testValidDouble()
    {
        ResolvedFunction doubleAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(DOUBLE));
        assertAggregation(
                metadata,
                doubleAgg,
                2.0,
                createDoublesBlock(null, 2.0));
    }

    @Test
    public void testNullString()
    {
        ResolvedFunction stringAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(VARCHAR));
        assertAggregation(
                metadata,
                stringAgg,
                null,
                createStringsBlock(null, null));
    }

    @Test
    public void testValidString()
    {
        ResolvedFunction stringAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(VARCHAR));
        assertAggregation(
                metadata,
                stringAgg,
                "a",
                createStringsBlock("a", "a"));
    }

    @Test
    public void testNullArray()
    {
        ResolvedFunction arrayAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(new ArrayType(BIGINT)));
        assertAggregation(
                metadata,
                arrayAgg,
                null,
                createArrayBigintBlock(Arrays.asList(null, null, null, null)));
    }

    @Test
    public void testValidArray()
    {
        ResolvedFunction arrayAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(new ArrayType(BIGINT)));
        assertAggregation(
                metadata,
                arrayAgg,
                ImmutableList.of(23L, 45L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L))));
    }

    @Test
    public void testValidInt()
    {
        ResolvedFunction arrayAgg = metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(INTEGER));
        assertAggregation(
                metadata,
                arrayAgg,
                3,
                createIntsBlock(3, 3, null));
    }
}
