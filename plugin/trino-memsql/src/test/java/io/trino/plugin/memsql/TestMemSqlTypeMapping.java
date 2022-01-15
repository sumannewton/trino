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
package io.trino.plugin.memsql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataType;
import io.trino.testing.datatype.DataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.STRICT;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_DEFAULT_SCALE;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_MAPPING;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_ROUNDING_MODE;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.memsql.MemSqlClient.MEMSQL_VARCHAR_MAX_LENGTH;
import static io.trino.plugin.memsql.MemSqlQueryRunner.createMemSqlQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.datatype.DataType.bigintDataType;
import static io.trino.testing.datatype.DataType.charDataType;
import static io.trino.testing.datatype.DataType.dataType;
import static io.trino.testing.datatype.DataType.dateDataType;
import static io.trino.testing.datatype.DataType.decimalDataType;
import static io.trino.testing.datatype.DataType.doubleDataType;
import static io.trino.testing.datatype.DataType.formatStringLiteral;
import static io.trino.testing.datatype.DataType.integerDataType;
import static io.trino.testing.datatype.DataType.realDataType;
import static io.trino.testing.datatype.DataType.smallintDataType;
import static io.trino.testing.datatype.DataType.stringDataType;
import static io.trino.testing.datatype.DataType.tinyintDataType;
import static io.trino.testing.datatype.DataType.varcharDataType;
import static io.trino.type.JsonType.JSON;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Arrays.asList;

public class TestMemSqlTypeMapping
        extends AbstractTestQueryFramework
{
    private static final String CHARACTER_SET_UTF8 = "CHARACTER SET utf8";

    protected TestingMemSqlServer memSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        memSqlServer = new TestingMemSqlServer();
        return createMemSqlQueryRunner(memSqlServer, ImmutableMap.of(), ImmutableMap.of(), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        memSqlServer.close();
    }

    @Test
    public void testBasicTypes()
    {
        DataTypeTest.create()
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(tinyintDataType(), (byte) 125)
                .addRoundTrip(doubleDataType(), 123.45d)
                .addRoundTrip(realDataType(), 123.45f)
                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testFloat()
    {
        singlePrecisionFloatingPointTests(realDataType())
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_float"));
        singlePrecisionFloatingPointTests(memSqlFloatDataType())
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_float"));
    }

    private static DataTypeTest singlePrecisionFloatingPointTests(DataType<Float> floatType)
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MemSQL
        return DataTypeTest.create()
                .addRoundTrip(floatType, 3.14f)
                // TODO Overeagerly rounded by MemSQL to 3.14159
                // .addRoundTrip(floatType, 3.1415927f)
                .addRoundTrip(floatType, null);
    }

    @Test
    public void testDouble()
    {
        doublePrecisionFloatingPointTests(doubleDataType())
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"));
        doublePrecisionFloatingPointTests(memSqlDoubleDataType())
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_double"));
    }

    private static DataTypeTest doublePrecisionFloatingPointTests(DataType<Double> doubleType)
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MemSQL
        return DataTypeTest.create()
                .addRoundTrip(doubleType, 1.0e100d)
                .addRoundTrip(doubleType, 123.456E10)
                .addRoundTrip(doubleType, null);
    }

    @Test
    public void testUnsignedTypes()
    {
        DataType<Short> memSqlUnsignedTinyInt = DataType.dataType("TINYINT UNSIGNED", SMALLINT, Objects::toString);
        DataType<Integer> memSqlUnsignedSmallInt = DataType.dataType("SMALLINT UNSIGNED", INTEGER, Objects::toString);
        DataType<Long> memSqlUnsignedInt = DataType.dataType("INT UNSIGNED", BIGINT, Objects::toString);
        DataType<Long> memSqlUnsignedInteger = DataType.dataType("INTEGER UNSIGNED", BIGINT, Objects::toString);
        DataType<BigDecimal> memSqlUnsignedBigint = DataType.dataType("BIGINT UNSIGNED", createDecimalType(20), Objects::toString);

        DataTypeTest.create()
                .addRoundTrip(memSqlUnsignedTinyInt, (short) 255)
                .addRoundTrip(memSqlUnsignedSmallInt, 65_535)
                .addRoundTrip(memSqlUnsignedInt, 4_294_967_295L)
                .addRoundTrip(memSqlUnsignedInteger, 4_294_967_295L)
                .addRoundTrip(memSqlUnsignedBigint, new BigDecimal("18446744073709551615"))
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_unsigned"));
    }

    @Test
    public void testMemsqlCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_decimal"));
    }

    @Test
    public void testTrinoCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));
    }

    private DataTypeTest decimalTests()
    {
        return DataTypeTest.create()
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("193"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("19"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("-193"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.0"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.1"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("-10.1"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("123456789.3"))
                .addRoundTrip(decimalDataType(24, 4), new BigDecimal("12345678901234567890.31"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("-27182818284590452353602874713526624977"));
    }

    @Test
    public void testDecimalExceedingPrecisionMax()
    {
        testUnsupportedDataType("decimal(50,0)");
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithExceedingIntegerValues()
    {
        try (TestTable testTable = new TestTable(
                memSqlServer::execute,
                "tpch.test_exceeding_max_decimal",
                "(d_col decimal(65,25))",
                asList("1234567890123456789012345678901234567890.123456789", "-1234567890123456789012345678901234567890.123456789"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'varchar')");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES ('1234567890123456789012345678901234567890.1234567890000000000000000'), ('-1234567890123456789012345678901234567890.1234567890000000000000000')");
        }
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithNonExceedingIntegerValues()
    {
        try (TestTable testTable = new TestTable(
                memSqlServer::execute,
                "tpch.test_exceeding_max_decimal",
                "(d_col decimal(60,20))",
                asList("123456789012345678901234567890.123456789012345", "-123456789012345678901234567890.123456789012345"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (123456789012345678901234567890), (-123456789012345678901234567890)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,8)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (123456789012345678901234567890.12345679), (-123456789012345678901234567890.12345679)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 22),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,20)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 20),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 9),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'varchar')");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES ('123456789012345678901234567890.12345678901234500000'), ('-123456789012345678901234567890.12345678901234500000')");
        }
    }

    @Test(dataProvider = "testDecimalExceedingPrecisionMaxProvider")
    public void testDecimalExceedingPrecisionMaxWithSupportedValues(int typePrecision, int typeScale)
    {
        try (TestTable testTable = new TestTable(
                memSqlServer::execute,
                "tpch.test_exceeding_max_decimal",
                format("(d_col decimal(%d,%d))", typePrecision, typeScale),
                asList("12.01", "-12.01", "123", "-123", "1.12345678", "-1.12345678"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12), (-12), (123), (-123), (1), (-1)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 3),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,3)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.123), (-1.123)");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,8)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 9),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
        }
    }

    @DataProvider
    public Object[][] testDecimalExceedingPrecisionMaxProvider()
    {
        return new Object[][] {
                {40, 8},
                {50, 10},
        };
    }

    private Session sessionWithDecimalMappingAllowOverflow(RoundingMode roundingMode, int scale)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("memsql", DECIMAL_MAPPING, ALLOW_OVERFLOW.name())
                .setCatalogSessionProperty("memsql", DECIMAL_ROUNDING_MODE, roundingMode.name())
                .setCatalogSessionProperty("memsql", DECIMAL_DEFAULT_SCALE, Integer.valueOf(scale).toString())
                .build();
    }

    private Session sessionWithDecimalMappingStrict(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("memsql", DECIMAL_MAPPING, STRICT.name())
                .setCatalogSessionProperty("memsql", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    @Test
    public void testTrinoCreatedParameterizedChar()
    {
        memSqlCharTypeTest()
                .execute(getQueryRunner(), trinoCreateAsSelect("memsql_test_parameterized_char"));
    }

    @Test
    public void testMemSqlCreatedParameterizedChar()
    {
        memSqlCharTypeTest()
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_char"));
    }

    private DataTypeTest memSqlCharTypeTest()
    {
        return DataTypeTest.create()
                .addRoundTrip(charDataType("char", 1), "")
                .addRoundTrip(charDataType("char", 1), "a")
                .addRoundTrip(charDataType(1), "")
                .addRoundTrip(charDataType(1), "a")
                .addRoundTrip(charDataType(8), "abc")
                .addRoundTrip(charDataType(8), "12345678")
                .addRoundTrip(charDataType(255), "a".repeat(255));
    }

    @Test
    public void testMemSqlCreatedParameterizedCharUnicode()
    {
        DataTypeTest.create()
                .addRoundTrip(charDataType(1, CHARACTER_SET_UTF8), "\u653b")
                .addRoundTrip(charDataType(5, CHARACTER_SET_UTF8), "\u653b\u6bbb")
                .addRoundTrip(charDataType(5, CHARACTER_SET_UTF8), "\u653b\u6bbb\u6a5f\u52d5\u968a")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_varchar"));
    }

    @Test
    public void testTrinoCreatedParameterizedVarchar()
    {
        DataTypeTest.create()
                .addRoundTrip(stringDataType("varchar(10)", createVarcharType(10)), "text_a")
                .addRoundTrip(stringDataType("varchar(255)", createVarcharType(255)), "text_b")
                .addRoundTrip(stringDataType("varchar(256)", createVarcharType(256)), "text_c")
                .addRoundTrip(stringDataType("varchar(" + MEMSQL_VARCHAR_MAX_LENGTH + ")", createVarcharType(MEMSQL_VARCHAR_MAX_LENGTH)), "text_memsql_max")
                // types larger than max VARCHAR(n) for MemSQL get mapped to one of TEXT/MEDIUMTEXT/LONGTEXT
                .addRoundTrip(stringDataType("varchar(" + (MEMSQL_VARCHAR_MAX_LENGTH + 1) + ")", createVarcharType(65535)), "text_memsql_larger_than_max")
                .addRoundTrip(stringDataType("varchar(65535)", createVarcharType(65535)), "text_d")
                .addRoundTrip(stringDataType("varchar(65536)", createVarcharType(16777215)), "text_e")
                .addRoundTrip(stringDataType("varchar(16777215)", createVarcharType(16777215)), "text_f")
                .addRoundTrip(stringDataType("varchar(16777216)", createUnboundedVarcharType()), "text_g")
                .addRoundTrip(stringDataType("varchar(" + VarcharType.MAX_LENGTH + ")", createUnboundedVarcharType()), "text_h")
                .addRoundTrip(varcharDataType(), "unbounded")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_parameterized_varchar"));
    }

    @Test
    public void testMemSqlCreatedParameterizedVarchar()
    {
        DataTypeTest.create()
                .addRoundTrip(stringDataType("tinytext", createVarcharType(255)), "a")
                .addRoundTrip(stringDataType("text", createVarcharType(65535)), "b")
                .addRoundTrip(stringDataType("mediumtext", createVarcharType(16777215)), "c")
                .addRoundTrip(stringDataType("longtext", createUnboundedVarcharType()), "d")
                .addRoundTrip(varcharDataType(32), "e")
                .addRoundTrip(varcharDataType(15000), "f")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_varchar"));
    }

    @Test
    public void testMemSqlCreatedParameterizedVarcharUnicode()
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        DataTypeTest.create()
                .addRoundTrip(stringDataType("tinytext " + CHARACTER_SET_UTF8, createVarcharType(255)), sampleUnicodeText)
                .addRoundTrip(stringDataType("text " + CHARACTER_SET_UTF8, createVarcharType(65535)), sampleUnicodeText)
                .addRoundTrip(stringDataType("mediumtext " + CHARACTER_SET_UTF8, createVarcharType(16777215)), sampleUnicodeText)
                .addRoundTrip(stringDataType("longtext " + CHARACTER_SET_UTF8, createUnboundedVarcharType()), sampleUnicodeText)
                .addRoundTrip(varcharDataType(sampleUnicodeText.length(), CHARACTER_SET_UTF8), sampleUnicodeText)
                .addRoundTrip(varcharDataType(32, CHARACTER_SET_UTF8), sampleUnicodeText)
                .addRoundTrip(varcharDataType(20000, CHARACTER_SET_UTF8), sampleUnicodeText)
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testDate()
    {
        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");

        ZoneId someZone = ZoneId.of("Europe/Vilnius");

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            dateTestCases(memSqlDateDataType(value -> formatStringLiteral(value.toString())), jvmZone, someZone)
                    .execute(getQueryRunner(), session, memSqlCreateAndInsert("tpch.test_date"));
            dateTestCases(dateDataType(), jvmZone, someZone)
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"));
            dateTestCases(dateDataType(), jvmZone, someZone)
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(getSession(), "test_date"));
            dateTestCases(dateDataType(), jvmZone, someZone)
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"));
        }
    }

    private DataTypeTest dateTestCases(DataType<LocalDate> dateDataType, ZoneId jvmZone, ZoneId someZone)
    {
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        verify(jvmZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay()).isEmpty());

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        verify(someZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay()).isEmpty());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        verify(someZone.getRules().getValidOffsets(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1)).size() == 2);

        return DataTypeTest.create()
                .addRoundTrip(dateDataType, LocalDate.of(1952, 4, 3)) // before epoch
                .addRoundTrip(dateDataType, LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType, LocalDate.of(1970, 2, 3))
                .addRoundTrip(dateDataType, LocalDate.of(2017, 7, 1)) // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType, LocalDate.of(2017, 1, 1)) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType, dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(dateDataType, dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(dateDataType, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);
    }

    @Test
    public void testDatetime()
    {
        // TODO (https://github.com/trinodb/trino/issues/5450) MemSQL datetime is not correctly read (see comment in StandardColumnMappings.timestampColumnMappingUsingSqlTimestamp)
        throw new SkipException("TODO");
    }

    @Test
    public void testTimestamp()
    {
        // TODO (https://github.com/trinodb/trino/issues/5450) MemSQL timestamp is not correctly read (see comment in StandardColumnMappings.timestampColumnMappingUsingSqlTimestamp)
        throw new SkipException("TODO");
    }

    @Test
    public void testJson()
    {
        jsonTestCases(memSqlJsonDataType(value -> "JSON " + formatStringLiteral(value)))
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_json"));
        // MemSQL doesn't support CAST to JSON but accepts string literals as JSON values
        jsonTestCases(memSqlJsonDataType(value -> format("%s", formatStringLiteral(value))))
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.mysql_test_json"));
    }

    private DataTypeTest jsonTestCases(DataType<String> jsonDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(jsonDataType, "{}")
                .addRoundTrip(jsonDataType, null)
                .addRoundTrip(jsonDataType, "null")
                .addRoundTrip(jsonDataType, "123.4")
                .addRoundTrip(jsonDataType, "\"abc\"")
                .addRoundTrip(jsonDataType, "\"text with ' apostrophes\"")
                .addRoundTrip(jsonDataType, "\"\"")
                .addRoundTrip(jsonDataType, "{\"a\":1,\"b\":2}")
                .addRoundTrip(jsonDataType, "{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}")
                .addRoundTrip(jsonDataType, "[]");
    }

    private void testUnsupportedDataType(String databaseDataType)
    {
        SqlExecutor jdbcSqlExecutor = memSqlServer::execute;
        jdbcSqlExecutor.execute(format("CREATE TABLE tpch.test_unsupported_data_type(supported_column varchar(5), unsupported_column %s)", databaseDataType));
        try {
            assertQuery(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'tpch' AND TABLE_NAME = 'test_unsupported_data_type'",
                    "VALUES 'supported_column'"); // no 'unsupported_column'
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.test_unsupported_data_type");
        }
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup memSqlCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(memSqlServer::execute, tableNamePrefix);
    }

    private static DataType<LocalDate> memSqlDateDataType(Function<LocalDate, String> toLiteral)
    {
        return dataType("date", DATE, toLiteral);
    }

    private static DataType<String> memSqlJsonDataType(Function<String, String> toLiteral)
    {
        return dataType("json", JSON, toLiteral);
    }

    private static DataType<Float> memSqlFloatDataType()
    {
        return dataType("float", REAL, Object::toString);
    }

    private static DataType<Double> memSqlDoubleDataType()
    {
        return dataType("double precision", DOUBLE, Object::toString);
    }
}
