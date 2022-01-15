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
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createArrayBigintBlock;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createLongDecimalsBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createShortDecimalsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertNotNull;

public class TestMinMaxByAggregation
{
    private static final Metadata METADATA = createTestMetadataManager();

    @Test
    public void testAllRegistered()
    {
        Set<Type> orderableTypes = getTypes().stream()
                .filter(Type::isOrderable)
                .collect(toImmutableSet());

        for (Type keyType : orderableTypes) {
            for (Type valueType : getTypes()) {
                assertNotNull(METADATA.getAggregateFunctionImplementation(METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(valueType, keyType))));
                assertNotNull(METADATA.getAggregateFunctionImplementation(METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(valueType, keyType))));
            }
        }
    }

    private static List<Type> getTypes()
    {
        return new ImmutableList.Builder<Type>()
                .addAll(METADATA.getTypes())
                .add(VARCHAR)
                .add(createDecimalType(1))
                .add(RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR, DOUBLE)))
                .build();
    }

    @Test
    public void testMinUnknown()
    {
        ResolvedFunction unknownKey = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(UNKNOWN, DOUBLE));
        assertAggregation(
                METADATA,
                unknownKey,
                null,
                createBooleansBlock(null, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                METADATA,
                unknownKey,
                null,
                createDoublesBlock(1.0, 2.0),
                createBooleansBlock(null, null));
    }

    @Test
    public void testMaxUnknown()
    {
        ResolvedFunction unknownKey = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(UNKNOWN, DOUBLE));
        assertAggregation(
                METADATA,
                unknownKey,
                null,
                createBooleansBlock(null, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                METADATA,
                unknownKey,
                null,
                createDoublesBlock(1.0, 2.0),
                createBooleansBlock(null, null));
    }

    @Test
    public void testMinNull()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(DOUBLE, DOUBLE));
        assertAggregation(
                METADATA,
                function,
                1.0,
                createDoublesBlock(1.0, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                METADATA,
                function,
                10.0,
                createDoublesBlock(10.0, 9.0, 8.0, 11.0),
                createDoublesBlock(1.0, null, 2.0, null));
    }

    @Test
    public void testMaxNull()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(DOUBLE, DOUBLE));
        assertAggregation(
                METADATA,
                function,
                null,
                createDoublesBlock(1.0, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                METADATA,
                function,
                10.0,
                createDoublesBlock(8.0, 9.0, 10.0, 11.0),
                createDoublesBlock(-2.0, null, -1.0, null));
    }

    @Test
    public void testMinDoubleDouble()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(DOUBLE, DOUBLE));
        assertAggregation(
                METADATA,
                function,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));

        assertAggregation(
                METADATA,
                function,
                3.0,
                createDoublesBlock(3.0, 2.0, 5.0, 3.0),
                createDoublesBlock(1.0, 1.5, 2.0, 4.0));
    }

    @Test
    public void testMaxDoubleDouble()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(DOUBLE, DOUBLE));
        assertAggregation(
                METADATA,
                function,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));

        assertAggregation(
                METADATA,
                function,
                2.0,
                createDoublesBlock(3.0, 2.0, null),
                createDoublesBlock(1.0, 1.5, null));
    }

    @Test
    public void testMinVarcharDouble()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(DOUBLE, VARCHAR));
        assertAggregation(
                METADATA,
                function,
                100.0,
                createDoublesBlock(100.0, 1.0, 50.0, 2.0),
                createStringsBlock("a", "b", "c", "d"));

        assertAggregation(
                METADATA,
                function,
                -1.0,
                createDoublesBlock(100.0, 50.0, 2.0, -1.0),
                createStringsBlock("x", "y", "z", "a"));
    }

    @Test
    public void testMinDoubleVarchar()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(VARCHAR, DOUBLE));
        assertAggregation(
                METADATA,
                function,
                "z",
                createStringsBlock("z", "a", "x", "b"),
                createDoublesBlock(1.0, 2.0, 2.0, 3.0));

        assertAggregation(
                METADATA,
                function,
                "a",
                createStringsBlock("zz", "hi", "bb", "a"),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0));
    }

    @Test
    public void testMaxDoubleVarchar()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(VARCHAR, DOUBLE));
        assertAggregation(
                METADATA,
                function,
                "a",
                createStringsBlock("z", "a", null),
                createDoublesBlock(1.0, 2.0, null));

        assertAggregation(
                METADATA,
                function,
                "hi",
                createStringsBlock("zz", "hi", null, "a"),
                createDoublesBlock(0.0, 1.0, null, -1.0));

        assertAggregation(
                METADATA,
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createDoublesBlock(Double.NaN, 1.0, 2.0));

        assertAggregation(
                METADATA,
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createDoublesBlock(1.0, Double.NaN, 2.0));

        assertAggregation(
                METADATA,
                function,
                "b",
                createStringsBlock("a", "b", "c"),
                createDoublesBlock(1.0, 2.0, Double.NaN));
    }

    @Test
    public void testMinRealVarchar()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(VARCHAR, REAL));
        assertAggregation(
                METADATA,
                function,
                "z",
                createStringsBlock("z", "a", "x", "b"),
                createBlockOfReals(1.0f, 2.0f, 2.0f, 3.0f));

        assertAggregation(
                METADATA,
                function,
                "a",
                createStringsBlock("zz", "hi", "bb", "a"),
                createBlockOfReals(0.0f, 1.0f, 2.0f, -1.0f));

        assertAggregation(
                METADATA,
                function,
                "b",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(Float.NaN, 1.0f, 2.0f));

        assertAggregation(
                METADATA,
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(1.0f, Float.NaN, 2.0f));

        assertAggregation(
                METADATA,
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(1.0f, 2.0f, Float.NaN));
    }

    @Test
    public void testMaxRealVarchar()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(VARCHAR, REAL));
        assertAggregation(
                METADATA,
                function,
                "a",
                createStringsBlock("z", "a", null),
                createBlockOfReals(1.0f, 2.0f, null));

        assertAggregation(
                METADATA,
                function,
                "hi",
                createStringsBlock("zz", "hi", null, "a"),
                createBlockOfReals(0.0f, 1.0f, null, -1.0f));

        assertAggregation(
                METADATA,
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(Float.NaN, 1.0f, 2.0f));

        assertAggregation(
                METADATA,
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(1.0f, Float.NaN, 2.0f));

        assertAggregation(
                METADATA,
                function,
                "b",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(1.0f, 2.0f, Float.NaN));
    }

    @Test
    public void testMinLongLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(new ArrayType(BIGINT), BIGINT));
        assertAggregation(
                METADATA,
                function,
                ImmutableList.of(8L, 9L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(2L, 3L))),
                createLongsBlock(1L, 2L, 2L, 3L));

        assertAggregation(
                METADATA,
                function,
                ImmutableList.of(2L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(6L, 7L), ImmutableList.of(2L, 3L), ImmutableList.of(2L))),
                createLongsBlock(0L, 1L, 2L, -1L));
    }

    @Test
    public void testMinLongArrayLong()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(BIGINT, new ArrayType(BIGINT)));
        assertAggregation(
                METADATA,
                function,
                3L,
                createLongsBlock(1L, 2L, 2L, 3L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(1L, 1L))));

        assertAggregation(
                METADATA,
                function,
                -1L,
                createLongsBlock(0L, 1L, 2L, -1L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(6L, 7L), ImmutableList.of(-1L, -3L), ImmutableList.of(-1L))));
    }

    @Test
    public void testMaxLongArrayLong()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(BIGINT, new ArrayType(BIGINT)));
        assertAggregation(
                METADATA,
                function,
                1L,
                createLongsBlock(1L, 2L, 2L, 3L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(1L, 1L))));

        assertAggregation(
                METADATA,
                function,
                2L,
                createLongsBlock(0L, 1L, 2L, -1L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(-8L, 9L), ImmutableList.of(-6L, 7L), ImmutableList.of(-1L, -3L), ImmutableList.of(-1L))));
    }

    @Test
    public void testMaxLongLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(new ArrayType(BIGINT), BIGINT));
        assertAggregation(
                METADATA,
                function,
                ImmutableList.of(1L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), asList(1L, 2L), null)),
                createLongsBlock(1L, 2L, null));

        assertAggregation(
                METADATA,
                function,
                ImmutableList.of(2L, 3L),
                createArrayBigintBlock(asList(asList(3L, 4L), asList(2L, 3L), null, asList(1L, 2L))),
                createLongsBlock(0L, 1L, null, -1L));
    }

    @Test
    public void testMinLongDecimalDecimal()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(createDecimalType(19, 1), createDecimalType(19, 1)));
        assertAggregation(
                METADATA,
                function,
                SqlDecimal.of("2.2"),
                createLongDecimalsBlock("1.1", "2.2", "3.3"),
                createLongDecimalsBlock("1.2", "1.0", "2.0"));
    }

    @Test
    public void testMaxLongDecimalDecimal()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(createDecimalType(19, 1), createDecimalType(19, 1)));
        assertAggregation(
                METADATA,
                function,
                SqlDecimal.of("3.3"),
                createLongDecimalsBlock("1.1", "2.2", "3.3", "4.4"),
                createLongDecimalsBlock("1.2", "1.0", "2.0", "1.5"));
    }

    @Test
    public void testMinShortDecimalDecimal()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(createDecimalType(10, 1), createDecimalType(10, 1)));
        assertAggregation(
                METADATA,
                function,
                SqlDecimal.of("2.2"),
                createShortDecimalsBlock("1.1", "2.2", "3.3"),
                createShortDecimalsBlock("1.2", "1.0", "2.0"));
    }

    @Test
    public void testMaxShortDecimalDecimal()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(createDecimalType(10, 1), createDecimalType(10, 1)));
        assertAggregation(
                METADATA,
                function,
                SqlDecimal.of("3.3"),
                createShortDecimalsBlock("1.1", "2.2", "3.3", "4.4"),
                createShortDecimalsBlock("1.2", "1.0", "2.0", "1.5"));
    }

    @Test
    public void testMinBooleanVarchar()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(VARCHAR, BOOLEAN));
        assertAggregation(
                METADATA,
                function,
                "b",
                createStringsBlock("a", "b", "c"),
                createBooleansBlock(true, false, true));
    }

    @Test
    public void testMaxBooleanVarchar()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(VARCHAR, BOOLEAN));
        assertAggregation(
                METADATA,
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createBooleansBlock(false, false, true));
    }

    @Test
    public void testMinIntegerVarchar()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(VARCHAR, INTEGER));
        assertAggregation(
                METADATA,
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createIntsBlock(1, 2, 3));
    }

    @Test
    public void testMaxIntegerVarchar()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(VARCHAR, INTEGER));
        assertAggregation(
                METADATA,
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createIntsBlock(1, 2, 3));
    }

    @Test
    public void testMinBooleanLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(new ArrayType(BIGINT), BOOLEAN));
        assertAggregation(
                METADATA,
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, null)),
                createBooleansBlock(true, false, true));
    }

    @Test
    public void testMaxBooleanLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(new ArrayType(BIGINT), BOOLEAN));
        assertAggregation(
                METADATA,
                function,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createBooleansBlock(false, false, true));
    }

    @Test
    public void testMinLongVarchar()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(VARCHAR, BIGINT));
        assertAggregation(
                METADATA,
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createLongsBlock(1, 2, 3));
    }

    @Test
    public void testMaxLongVarchar()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(VARCHAR, BIGINT));
        assertAggregation(
                METADATA,
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createLongsBlock(1, 2, 3));
    }

    @Test
    public void testMinDoubleLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(new ArrayType(BIGINT), DOUBLE));
        assertAggregation(
                METADATA,
                function,
                asList(3L, 4L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(1.0, 2.0, 3.0));

        assertAggregation(
                METADATA,
                function,
                null,
                createArrayBigintBlock(asList(null, null, asList(2L, 2L))),
                createDoublesBlock(0.0, 1.0, 2.0));
    }

    @Test
    public void testMaxDoubleLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(new ArrayType(BIGINT), DOUBLE));
        assertAggregation(
                METADATA,
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(1.0, 2.0, null));

        assertAggregation(
                METADATA,
                function,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(0.0, 1.0, 2.0));
    }

    @Test
    public void testMinSliceLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(new ArrayType(BIGINT), VARCHAR));
        assertAggregation(
                METADATA,
                function,
                asList(3L, 4L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));

        assertAggregation(
                METADATA,
                function,
                null,
                createArrayBigintBlock(asList(null, null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));
    }

    @Test
    public void testMaxSliceLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(new ArrayType(BIGINT), VARCHAR));
        assertAggregation(
                METADATA,
                function,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));

        assertAggregation(
                METADATA,
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, null)),
                createStringsBlock("a", "b", "c"));
    }

    @Test
    public void testMinLongArrayLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(new ArrayType(BIGINT), new ArrayType(BIGINT)));
        assertAggregation(
                METADATA,
                function,
                asList(1L, 2L),
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMaxLongArrayLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(new ArrayType(BIGINT), new ArrayType(BIGINT)));
        assertAggregation(
                METADATA,
                function,
                asList(3L, 3L),
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMinLongArraySlice()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(VARCHAR, new ArrayType(BIGINT)));
        assertAggregation(
                METADATA,
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMaxLongArraySlice()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(VARCHAR, new ArrayType(BIGINT)));
        assertAggregation(
                METADATA,
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMinUnknownSlice()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(VARCHAR, UNKNOWN));
        assertAggregation(
                METADATA,
                function,
                null,
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMaxUnknownSlice()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(VARCHAR, UNKNOWN));
        assertAggregation(
                METADATA,
                function,
                null,
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMinUnknownLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("min_by"), fromTypes(new ArrayType(BIGINT), UNKNOWN));
        assertAggregation(
                METADATA,
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMaxUnknownLongArray()
    {
        ResolvedFunction function = METADATA.resolveFunction(TEST_SESSION, QualifiedName.of("max_by"), fromTypes(new ArrayType(BIGINT), UNKNOWN));
        assertAggregation(
                METADATA,
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(null, null, null)));
    }
}
