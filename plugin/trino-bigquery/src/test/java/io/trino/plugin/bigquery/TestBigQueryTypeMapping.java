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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;

public class TestBigQueryTypeMapping
        extends AbstractTestQueryFramework
{
    private BigQueryQueryRunner.BigQuerySqlExecutor bigQuerySqlExecutor;

    @BeforeClass(alwaysRun = true)
    public void initBigQueryExecutor()
    {
        bigQuerySqlExecutor = new BigQueryQueryRunner.BigQuerySqlExecutor();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    @Test
    public void testFloat()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("float64", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("float64", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("float64", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("float64", "CAST('NaN' AS float64)", DOUBLE, "nan()")
                .addRoundTrip("float64", "CAST('Infinity' AS float64)", DOUBLE, "+infinity()")
                .addRoundTrip("float64", "CAST('-Infinity' AS float64)", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.float"));
    }

    @Test
    public void testNumericMapping()
    {
        // Max precision is 29 when the scale is 0 in BigQuery
        // Valid scale range is between 0 and 9 in BigQuery

        // Use "NUMERIC 'value'" style because BigQuery doesn't accept parameterized cast "CAST (... AS NUMERIC(p, s))"
        SqlDataTypeTest.create()
                .addRoundTrip("NUMERIC(3, 0)", "NUMERIC '193'", createDecimalType(3, 0), "CAST(193 AS DECIMAL(3, 0))")
                .addRoundTrip("NUMERIC(3, 0)", "NUMERIC '19'", createDecimalType(3, 0), "CAST(19 AS DECIMAL(3, 0))")
                .addRoundTrip("NUMERIC(3, 0)", "NUMERIC '-193'", createDecimalType(3, 0), "CAST(-193 AS DECIMAL(3, 0))")
                .addRoundTrip("NUMERIC(3, 1)", "NUMERIC '10.0'", createDecimalType(3, 1), "CAST(10.0 AS DECIMAL(3, 1))")
                .addRoundTrip("NUMERIC(3, 1)", "NUMERIC '10.1'", createDecimalType(3, 1), "CAST(10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("NUMERIC(3, 1)", "NUMERIC '-10.1'", createDecimalType(3, 1), "CAST(-10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("NUMERIC(4, 2)", "NUMERIC '2'", createDecimalType(4, 2), "CAST(2 AS DECIMAL(4, 2))")
                .addRoundTrip("NUMERIC(4, 2)", "NUMERIC '2.3'", createDecimalType(4, 2), "CAST(2.3 AS DECIMAL(4, 2))")
                .addRoundTrip("NUMERIC(24, 2)", "NUMERIC '2'", createDecimalType(24, 2), "CAST(2 AS DECIMAL(24, 2))")
                .addRoundTrip("NUMERIC(24, 2)", "NUMERIC '2.3'", createDecimalType(24, 2), "CAST(2.3 AS DECIMAL(24, 2))")
                .addRoundTrip("NUMERIC(24, 2)", "NUMERIC '123456789.3'", createDecimalType(24, 2), "CAST(123456789.3 AS DECIMAL(24, 2))")
                .addRoundTrip("NUMERIC(24, 4)", "NUMERIC '12345678901234567890.31'", createDecimalType(24, 4), "CAST(12345678901234567890.31 AS DECIMAL(24, 4))")
                .addRoundTrip("NUMERIC(29, 0)", "NUMERIC '27182818284590452353602874713'", createDecimalType(29, 0), "CAST('27182818284590452353602874713' AS DECIMAL(29, 0))")
                .addRoundTrip("NUMERIC(29, 0)", "NUMERIC '-27182818284590452353602874713'", createDecimalType(29, 0), "CAST('-27182818284590452353602874713' AS DECIMAL(29, 0))")
                .addRoundTrip("NUMERIC(30, 5)", "NUMERIC '3141592653589793238462643.38327'", createDecimalType(30, 5), "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("NUMERIC(30, 5)", "NUMERIC '-3141592653589793238462643.38327'", createDecimalType(30, 5), "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("NUMERIC(38, 9)", "NUMERIC '100000000020000000001234567.123456789'", createDecimalType(38, 9), "CAST(100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(38, 9)", "NUMERIC '-100000000020000000001234567.123456789'", createDecimalType(38, 9), "CAST(-100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(10, 3)", "CAST(NULL AS NUMERIC)", createDecimalType(10, 3), "CAST(NULL AS DECIMAL(10, 3))")
                .addRoundTrip("NUMERIC(38, 9)", "CAST(NULL AS NUMERIC)", createDecimalType(38, 9), "CAST(NULL AS DECIMAL(38, 9))")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.numeric"));
    }

    @Test
    public void testDatetime()
    {
        SqlDataTypeTest.create()
                // before epoch
                .addRoundTrip("datetime", "datetime '1958-01-01 13:18:03.123'", createTimestampType(6), "TIMESTAMP '1958-01-01 13:18:03.123000'")
                // after epoch
                .addRoundTrip("datetime", "datetime '2019-03-18 10:01:17.987'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987000'")
                .addRoundTrip("datetime", "datetime '2018-10-28 01:33:17.456'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.456000'")
                .addRoundTrip("datetime", "datetime '2018-10-28 03:33:33.333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333000'")
                // epoch
                .addRoundTrip("datetime", "datetime '1970-01-01 00:00:00.000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000000'")
                .addRoundTrip("datetime", "datetime '1970-01-01 00:13:42.000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.000000'")
                .addRoundTrip("datetime", "datetime '2018-04-01 02:13:55.123'", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123000'")
                .addRoundTrip("datetime", "datetime '2018-03-25 03:17:17.000'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.000000'")
                .addRoundTrip("datetime", "datetime '1986-01-01 00:13:07.000'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.000000'")

                // same as above but with higher precision
                .addRoundTrip("datetime", "datetime '1958-01-01 13:18:03.123456'", createTimestampType(6), "TIMESTAMP '1958-01-01 13:18:03.123456'")
                .addRoundTrip("datetime", "datetime '2019-03-18 10:01:17.987654'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987654'")
                .addRoundTrip("datetime", "datetime '2018-10-28 01:33:17.123456'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.123456'")
                .addRoundTrip("datetime", "datetime '2018-10-28 03:33:33.333333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'")
                .addRoundTrip("datetime", "datetime '1970-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000000'")
                .addRoundTrip("datetime", "datetime '1970-01-01 00:13:42.123456'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.123456'")
                .addRoundTrip("datetime", "datetime '2018-04-01 02:13:55.123456'", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123456'")
                .addRoundTrip("datetime", "datetime '2018-03-25 03:17:17.456789'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.456789'")
                .addRoundTrip("datetime", "datetime '1986-01-01 00:13:07.456789'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.456789'")
                .addRoundTrip("datetime", "datetime '2021-09-07 23:59:59.999999'", createTimestampType(6), "TIMESTAMP '2021-09-07 23:59:59.999999'")

                // test arbitrary time for all supported precisions
                .addRoundTrip("datetime", "datetime '1970-01-01 00:00:01'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.000000'")
                .addRoundTrip("datetime", "datetime '1970-01-01 00:00:01.1'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.100000'")
                .addRoundTrip("datetime", "datetime '1970-01-01 00:00:01.12'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.120000'")
                .addRoundTrip("datetime", "datetime '1970-01-01 00:00:01.123'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.123000'")
                .addRoundTrip("datetime", "datetime '1970-01-01 00:00:01.1234'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.123400'")
                .addRoundTrip("datetime", "datetime '1970-01-01 00:00:01.12345'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.123450'")
                .addRoundTrip("datetime", "datetime '1970-01-01 00:00:01.123456'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.123456'")

                // negative epoch
                .addRoundTrip("datetime", "datetime '1969-12-31 23:59:59.999995'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999995'")
                .addRoundTrip("datetime", "datetime '1969-12-31 23:59:59.999949'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999949'")
                .addRoundTrip("datetime", "datetime '1969-12-31 23:59:59.999994'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999994'")

                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.datetime"));
    }

    @Test
    public void testTime()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("time", "'00:00:00'", createTimeType(6), "TIME '00:00:00.000000'")
                .addRoundTrip("time", "'00:00:00.000000'", createTimeType(6), "TIME '00:00:00.000000'")
                .addRoundTrip("time", "'00:00:00.123456'", createTimeType(6), "TIME '00:00:00.123456'")
                .addRoundTrip("time", "'12:34:56'", createTimeType(6), "TIME '12:34:56.000000'")
                .addRoundTrip("time", "'12:34:56.123456'", createTimeType(6), "TIME '12:34:56.123456'")

                // maximal value for a precision
                .addRoundTrip("time", "'23:59:59'", createTimeType(6), "TIME '23:59:59.000000'")
                .addRoundTrip("time", "'23:59:59.9'", createTimeType(6), "TIME '23:59:59.900000'")
                .addRoundTrip("time", "'23:59:59.99'", createTimeType(6), "TIME '23:59:59.990000'")
                .addRoundTrip("time", "'23:59:59.999'", createTimeType(6), "TIME '23:59:59.999000'")
                .addRoundTrip("time", "'23:59:59.9999'", createTimeType(6), "TIME '23:59:59.999900'")
                .addRoundTrip("time", "'23:59:59.99999'", createTimeType(6), "TIME '23:59:59.999990'")
                .addRoundTrip("time", "'23:59:59.999999'", createTimeType(6), "TIME '23:59:59.999999'")

                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.time"));
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.000000 Asia/Kathmandu'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1969-12-31 18:30:00.000000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.000000+02:17'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1969-12-31 21:43:00.000000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.000000-07:31'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 07:31:00.000000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1958-01-01 13:18:03.123456 UTC'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1958-01-01 13:18:03.123456 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1958-01-01 13:18:03.123000 Asia/Kathmandu'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1958-01-01 07:48:03.123000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1958-01-01 13:18:03.123000+02:17'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1958-01-01 11:01:03.123000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1958-01-01 13:18:03.123000-07:31'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1958-01-01 20:49:03.123000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2019-03-18 10:01:17.987654 UTC'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '2019-03-18 10:01:17.987654 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2019-03-18 10:01:17.987000 Asia/Kathmandu'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '2019-03-18 04:16:17.987000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2019-03-18 10:01:17.987000+02:17'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '2019-03-18 07:44:17.987000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2019-03-18 10:01:17.987000-07:31'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '2019-03-18 17:32:17.987000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2021-09-07 23:59:59.999999-00:00'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '2021-09-07 23:59:59.999999 UTC'")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.timestamp_tz"));
    }

    private DataSetup bigqueryCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(getBigQuerySqlExecutor(), tableNamePrefix);
    }

    private SqlExecutor getBigQuerySqlExecutor()
    {
        return sql -> bigQuerySqlExecutor.execute(sql);
    }
}
