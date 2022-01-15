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
package io.trino.plugin.pinot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.trino.Session;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.shaded.org.bouncycastle.util.encoders.Hex;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.testng.Assert.assertEquals;

public class TestPinotIntegrationSmokeTest
        // TODO extend BaseConnectorTest
        extends AbstractTestQueryFramework
{
    private static final int MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES = 11;
    private static final int MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES = 12;
    // If a broker query does not supply a limit, pinot defaults to 10 rows
    private static final int DEFAULT_PINOT_LIMIT_FOR_BROKER_QUERIES = 10;
    private static final String ALL_TYPES_TABLE = "alltypes";
    private static final String MIXED_CASE_COLUMN_NAMES_TABLE = "mixed_case";
    private static final String MIXED_CASE_DISTINCT_TABLE = "mixed_case_distinct";
    private static final String TOO_MANY_ROWS_TABLE = "too_many_rows";
    private static final String TOO_MANY_BROKER_ROWS_TABLE = "too_many_broker_rows";
    private static final String JSON_TABLE = "my_table";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingKafka kafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        kafka.start();
        TestingPinotCluster pinot = closeAfterClass(new TestingPinotCluster(kafka.getNetwork()));
        pinot.start();

        // Create and populate the all_types topic and table
        kafka.createTopic(ALL_TYPES_TABLE);

        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> allTypesRecordsBuilder = ImmutableList.builder();
        for (int i = 0, step = 1200; i < MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES - 2; i++) {
            int offset = i * step;
            allTypesRecordsBuilder.add(new ProducerRecord<>(ALL_TYPES_TABLE, "key" + i * step,
                    createTestRecord(
                            Arrays.asList("string_" + (offset), "string1_" + (offset + 1), "string2_" + (offset + 2)),
                            Arrays.asList(false, true, true),
                            Arrays.asList(54, -10001, 1000),
                            Arrays.asList(-7.33F + i, Float.POSITIVE_INFINITY, 17.034F + i),
                            Arrays.asList(-17.33D + i, .00014D - i, 10596.034D + i),
                            Arrays.asList(-3147483647L + i, 12L - i, 4147483647L + i),
                            Instant.parse("2021-05-10T00:00:00.00Z").plusMillis(offset).toEpochMilli())));
        }

        allTypesRecordsBuilder.add(new ProducerRecord<>(ALL_TYPES_TABLE, null, createNullRecord()));
        allTypesRecordsBuilder.add(new ProducerRecord<>(ALL_TYPES_TABLE, null, createArrayNullRecord()));
        kafka.sendMessages(allTypesRecordsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));

        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("alltypes_schema.json"), ALL_TYPES_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("alltypes_realtimeSpec.json"), ALL_TYPES_TABLE);

        // Create and populate mixed case table and topic
        kafka.createTopic(MIXED_CASE_COLUMN_NAMES_TABLE);
        Schema mixedCaseAvroSchema = SchemaBuilder.record(MIXED_CASE_COLUMN_NAMES_TABLE).fields()
                .name("stringCol").type().stringType().noDefault()
                .name("longCol").type().optional().longType()
                .name("updatedAt").type().longType().noDefault()
                .endRecord();

        List<ProducerRecord<String, GenericRecord>> mixedCaseProducerRecords = ImmutableList.<ProducerRecord<String, GenericRecord>>builder()
                .add(new ProducerRecord<>(MIXED_CASE_COLUMN_NAMES_TABLE, "key0", new GenericRecordBuilder(mixedCaseAvroSchema)
                        .set("stringCol", "string_0")
                        .set("longCol", 0L)
                        .set("updatedAt", Instant.parse("2021-05-10T00:00:00.00Z").toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_COLUMN_NAMES_TABLE, "key1", new GenericRecordBuilder(mixedCaseAvroSchema)
                        .set("stringCol", "string_1")
                        .set("longCol", 1L)
                        .set("updatedAt", Instant.parse("2021-05-10T00:00:00.00Z").plusMillis(1000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_COLUMN_NAMES_TABLE, "key2", new GenericRecordBuilder(mixedCaseAvroSchema)
                        .set("stringCol", "string_2")
                        .set("longCol", 2L)
                        .set("updatedAt", Instant.parse("2021-05-10T00:00:00.00Z").plusMillis(2000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_COLUMN_NAMES_TABLE, "key3", new GenericRecordBuilder(mixedCaseAvroSchema)
                        .set("stringCol", "string_3")
                        .set("longCol", 3L)
                        .set("updatedAt", Instant.parse("2021-05-10T00:00:00.00Z").plusMillis(3000).toEpochMilli())
                        .build()))
                .build();

        kafka.sendMessages(mixedCaseProducerRecords.stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("mixed_case_schema.json"), MIXED_CASE_COLUMN_NAMES_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("mixed_case_realtimeSpec.json"), MIXED_CASE_COLUMN_NAMES_TABLE);

        // Create and populate mixed case distinct table and topic
        kafka.createTopic(MIXED_CASE_DISTINCT_TABLE);
        Schema mixedCaseDistinctAvroSchema = SchemaBuilder.record(MIXED_CASE_DISTINCT_TABLE).fields()
                .name("string_col").type().stringType().noDefault()
                .name("updated_at").type().longType().noDefault()
                .endRecord();

        List<ProducerRecord<String, GenericRecord>> mixedCaseDistinctProducerRecords = ImmutableList.<ProducerRecord<String, GenericRecord>>builder()
                .add(new ProducerRecord<>(MIXED_CASE_DISTINCT_TABLE, "key0", new GenericRecordBuilder(mixedCaseDistinctAvroSchema)
                        .set("string_col", "A")
                        .set("updated_at", Instant.parse("2021-05-10T00:00:00.00Z").toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_DISTINCT_TABLE, "key1", new GenericRecordBuilder(mixedCaseDistinctAvroSchema)
                        .set("string_col", "a")
                        .set("updated_at", Instant.parse("2021-05-10T00:00:00.00Z").plusMillis(1000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_DISTINCT_TABLE, "key2", new GenericRecordBuilder(mixedCaseDistinctAvroSchema)
                        .set("string_col", "B")
                        .set("updated_at", Instant.parse("2021-05-10T00:00:00.00Z").plusMillis(2000).toEpochMilli())
                        .build()))
                .add(new ProducerRecord<>(MIXED_CASE_DISTINCT_TABLE, "key3", new GenericRecordBuilder(mixedCaseDistinctAvroSchema)
                        .set("string_col", "b")
                        .set("updated_at", Instant.parse("2021-05-10T00:00:00.00Z").plusMillis(3000).toEpochMilli())
                        .build()))
                .build();

        kafka.sendMessages(mixedCaseDistinctProducerRecords.stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("mixed_case_distinct_schema.json"), MIXED_CASE_DISTINCT_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("mixed_case_distinct_realtimeSpec.json"), MIXED_CASE_DISTINCT_TABLE);

        // Create and populate too many rows table and topic
        kafka.createTopic(TOO_MANY_ROWS_TABLE);
        Schema tooManyRowsAvroSchema = SchemaBuilder.record(TOO_MANY_ROWS_TABLE).fields()
                .name("string_col").type().optional().stringType()
                .name("updatedAt").type().optional().longType()
                .endRecord();

        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> tooManyRowsRecordsBuilder = ImmutableList.builder();
        for (int i = 0; i < MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 1; i++) {
            tooManyRowsRecordsBuilder.add(new ProducerRecord<>(TOO_MANY_ROWS_TABLE, "key" + i, new GenericRecordBuilder(tooManyRowsAvroSchema)
                    .set("string_col", "string_" + i)
                    .set("updatedAt", Instant.parse("2021-05-10T00:00:00.00Z").plusMillis(i * 1000).toEpochMilli())
                    .build()));
        }
        // Add a null row, verify it was not ingested as pinot does not accept null time column values.
        // The data is verified in testBrokerQueryWithTooManyRowsForSegmentQuery
        tooManyRowsRecordsBuilder.add(new ProducerRecord<>(TOO_MANY_ROWS_TABLE, "key" + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES, new GenericRecordBuilder(tooManyRowsAvroSchema).build()));
        kafka.sendMessages(tooManyRowsRecordsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("too_many_rows_schema.json"), TOO_MANY_ROWS_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("too_many_rows_realtimeSpec.json"), TOO_MANY_ROWS_TABLE);

        // Create and populate too many broker rows table and topic
        kafka.createTopic(TOO_MANY_BROKER_ROWS_TABLE);
        Schema tooManyBrokerRowsAvroSchema = SchemaBuilder.record(TOO_MANY_BROKER_ROWS_TABLE).fields()
                .name("string_col").type().optional().stringType()
                .name("updatedAt").type().optional().longType()
                .endRecord();

        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> tooManyBrokerRowsRecordsBuilder = ImmutableList.builder();
        for (int i = 0; i < MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES + 1; i++) {
            tooManyBrokerRowsRecordsBuilder.add(new ProducerRecord<>(TOO_MANY_BROKER_ROWS_TABLE, "key" + i, new GenericRecordBuilder(tooManyBrokerRowsAvroSchema)
                    .set("string_col", "string_" + i)
                    .set("updatedAt", Instant.parse("2021-05-10T00:00:00.00Z").plusMillis(i * 1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(tooManyBrokerRowsRecordsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("too_many_broker_rows_schema.json"), TOO_MANY_BROKER_ROWS_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("too_many_broker_rows_realtimeSpec.json"), TOO_MANY_BROKER_ROWS_TABLE);

        // Create json table
        kafka.createTopic(JSON_TABLE);
        long key = 0L;
        kafka.sendMessages(Stream.of(
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor1", "Los Angeles", Arrays.asList("foo1", "bar1", "baz1"), Arrays.asList(5, 6, 7), Arrays.asList(3.5F, 5.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 4)),
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor2", "New York", Arrays.asList("foo2", "bar1", "baz1"), Arrays.asList(6, 7, 8), Arrays.asList(4.5F, 6.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 6)),
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor3", "Los Angeles", Arrays.asList("foo3", "bar2", "baz1"), Arrays.asList(7, 8, 9), Arrays.asList(5.5F, 7.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 8)),
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor4", "New York", Arrays.asList("foo4", "bar2", "baz2"), Arrays.asList(8, 9, 10), Arrays.asList(6.5F, 8.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 10)),
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor5", "Los Angeles", Arrays.asList("foo5", "bar3", "baz2"), Arrays.asList(9, 10, 11), Arrays.asList(7.5F, 9.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 12)),
                new ProducerRecord<>(JSON_TABLE, key++, TestingJsonRecord.of("vendor6", "Los Angeles", Arrays.asList("foo6", "bar3", "baz2"), Arrays.asList(10, 11, 12), Arrays.asList(8.5F, 10.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 12)),
                new ProducerRecord<>(JSON_TABLE, key, TestingJsonRecord.of("vendor7", "Los Angeles", Arrays.asList("foo6", "bar3", "baz2"), Arrays.asList(10, 11, 12), Arrays.asList(9.5F, 10.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 12))));

        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("schema.json"), JSON_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("realtimeSpec.json"), JSON_TABLE);

        Map<String, String> pinotProperties = ImmutableMap.<String, String>builder()
                .put("pinot.controller-urls", pinot.getControllerConnectString())
                .put("pinot.max-rows-per-split-for-segment-queries", String.valueOf(MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .put("pinot.max-rows-for-broker-queries", String.valueOf(MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES))
                .build();

        return PinotQueryRunner.createPinotQueryRunner(
                ImmutableMap.of(),
                pinotProperties,
                Optional.of(binder -> newOptionalBinder(binder, PinotHostMapper.class).setBinding()
                        .toInstance(new TestingPinotHostMapper(pinot.getBrokerHostAndPort(), pinot.getServerHostAndPort()))));
    }

    private static Map<String, String> schemaRegistryAwareProducer(TestingKafka testingKafka)
    {
        return ImmutableMap.<String, String>builder()
                .put(SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString())
                .put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build();
    }

    private static GenericRecord createTestRecord(
            List<String> stringArrayColumn,
            List<Boolean> booleanArrayColumn,
            List<Integer> intArrayColumn,
            List<Float> floatArrayColumn,
            List<Double> doubleArrayColumn,
            List<Long> longArrayColumn,
            long updatedAtMillis)
    {
        Schema schema = getAllTypesAvroSchema();

        return new GenericRecordBuilder(schema)
                .set("string_col", stringArrayColumn.get(0))
                .set("bool_col", booleanArrayColumn.get(0))
                .set("bytes_col", Hex.toHexString(stringArrayColumn.get(0).getBytes(StandardCharsets.UTF_8)))
                .set("string_array_col", stringArrayColumn)
                .set("bool_array_col", booleanArrayColumn)
                .set("int_array_col", intArrayColumn)
                .set("int_array_col_with_pinot_default", intArrayColumn)
                .set("float_array_col", floatArrayColumn)
                .set("double_array_col", doubleArrayColumn)
                .set("long_array_col", longArrayColumn)
                .set("int_col", intArrayColumn.get(0))
                .set("float_col", floatArrayColumn.get(0))
                .set("double_col", doubleArrayColumn.get(0))
                .set("long_col", longArrayColumn.get(0))
                .set("updated_at", updatedAtMillis)
                .build();
    }

    private static GenericRecord createNullRecord()
    {
        Schema schema = getAllTypesAvroSchema();
        // Pinot does not transform the time column value to default null value
        return new GenericRecordBuilder(schema)
                .set("updated_at", 0L)
                .build();
    }

    private static GenericRecord createArrayNullRecord()
    {
        Schema schema = getAllTypesAvroSchema();
        List<String> stringList = Arrays.asList("string_0", null, "string_2", null, "string_4");
        List<Boolean> booleanList = Arrays.asList(true, null, false, null, true);
        List<Integer> integerList = new ArrayList<>();
        integerList.addAll(Arrays.asList(null, null, null, null, null));
        List<Integer> integerWithDefaultList = Arrays.asList(-1112, null, 753, null, -9238);
        List<Float> floatList = new ArrayList<>();
        floatList.add(null);
        List<Integer> doubleList = new ArrayList<>();
        doubleList.add(null);

        return new GenericRecordBuilder(schema)
                .set("string_col", "array_null")
                .set("string_array_col", stringList)
                .set("bool_array_col", booleanList)
                .set("int_array_col", integerList)
                .set("int_array_col_with_pinot_default", integerWithDefaultList)
                .set("float_array_col", floatList)
                .set("double_array_col", doubleList)
                .set("long_array_col", new ArrayList<>())
                .set("updated_at", 0L)
                .build();
    }

    private static Schema getAllTypesAvroSchema()
    {
        // Note:
        // The reason optional() is used is because the avro record can omit those fields.
        // Fields with nullable type are required to be included or have a default value.
        //
        // For example:
        // If "string_col" is set to type().nullable().stringType().noDefault()
        // the following error is returned: Field string_col type:UNION pos:0 not set and has no default value

        return SchemaBuilder.record("alltypes")
                .fields()
                .name("string_col").type().optional().stringType()
                .name("bool_col").type().optional().booleanType()
                .name("bytes_col").type().optional().stringType()
                .name("string_array_col").type().optional().array().items().nullable().stringType()
                .name("bool_array_col").type().optional().array().items().nullable().booleanType()
                .name("int_array_col").type().optional().array().items().nullable().intType()
                .name("int_array_col_with_pinot_default").type().optional().array().items().nullable().intType()
                .name("float_array_col").type().optional().array().items().nullable().floatType()
                .name("double_array_col").type().optional().array().items().nullable().doubleType()
                .name("long_array_col").type().optional().array().items().nullable().longType()
                .name("int_col").type().optional().intType()
                .name("float_col").type().optional().floatType()
                .name("double_col").type().optional().doubleType()
                .name("long_col").type().optional().longType()
                .name("updated_at").type().optional().longType()
                .endRecord();
    }

    private static class TestingJsonRecord
    {
        private final String vendor;
        private final String city;
        private final List<String> neighbors;
        private final List<Integer> luckyNumbers;
        private final List<Float> prices;
        private final List<Double> unluckyNumbers;
        private final List<Long> longNumbers;
        private final Integer luckyNumber;
        private final Float price;
        private final Double unluckyNumber;
        private final Long longNumber;
        private final long updatedAt;

        @JsonCreator
        public TestingJsonRecord(
                @JsonProperty("vendor") String vendor,
                @JsonProperty("city") String city,
                @JsonProperty("neighbors") List<String> neighbors,
                @JsonProperty("lucky_numbers") List<Integer> luckyNumbers,
                @JsonProperty("prices") List<Float> prices,
                @JsonProperty("unlucky_numbers") List<Double> unluckyNumbers,
                @JsonProperty("long_numbers") List<Long> longNumbers,
                @JsonProperty("lucky_number") Integer luckyNumber,
                @JsonProperty("price") Float price,
                @JsonProperty("unlucky_number") Double unluckyNumber,
                @JsonProperty("long_number") Long longNumber,
                @JsonProperty("updatedAt") long updatedAt)
        {
            this.vendor = requireNonNull(vendor, "vendor is null");
            this.city = requireNonNull(city, "city is null");
            this.neighbors = requireNonNull(neighbors, "neighbors is null");
            this.luckyNumbers = requireNonNull(luckyNumbers, "luckyNumbers is null");
            this.prices = requireNonNull(prices, "prices is null");
            this.unluckyNumbers = requireNonNull(unluckyNumbers, "unluckyNumbers is null");
            this.longNumbers = requireNonNull(longNumbers, "longNumbers is null");
            this.price = requireNonNull(price, "price is null");
            this.luckyNumber = requireNonNull(luckyNumber, "luckyNumber is null");
            this.unluckyNumber = requireNonNull(unluckyNumber, "unluckyNumber is null");
            this.longNumber = requireNonNull(longNumber, "longNumber is null");
            this.updatedAt = updatedAt;
        }

        @JsonProperty
        public String getVendor()
        {
            return vendor;
        }

        @JsonProperty
        public String getCity()
        {
            return city;
        }

        @JsonProperty
        public List<String> getNeighbors()
        {
            return neighbors;
        }

        @JsonProperty("lucky_numbers")
        public List<Integer> getLuckyNumbers()
        {
            return luckyNumbers;
        }

        @JsonProperty
        public List<Float> getPrices()
        {
            return prices;
        }

        @JsonProperty("unlucky_numbers")
        public List<Double> getUnluckyNumbers()
        {
            return unluckyNumbers;
        }

        @JsonProperty("long_numbers")
        public List<Long> getLongNumbers()
        {
            return longNumbers;
        }

        @JsonProperty("lucky_number")
        public Integer getLuckyNumber()
        {
            return luckyNumber;
        }

        @JsonProperty
        public Float getPrice()
        {
            return price;
        }

        @JsonProperty("unlucky_number")
        public Double getUnluckyNumber()
        {
            return unluckyNumber;
        }

        @JsonProperty("long_number")
        public Long getLongNumber()
        {
            return longNumber;
        }

        @JsonProperty
        public long getUpdatedAt()
        {
            return updatedAt;
        }

        public static Object of(
                String vendor,
                String city,
                List<String> neighbors,
                List<Integer> luckyNumbers,
                List<Float> prices,
                List<Double> unluckyNumbers,
                List<Long> longNumbers,
                long offset)
        {
            return new TestingJsonRecord(vendor, city, neighbors, luckyNumbers, prices, unluckyNumbers, longNumbers, luckyNumbers.get(0), prices.get(0), unluckyNumbers.get(0), longNumbers.get(0), Instant.now().plusMillis(offset).getEpochSecond());
        }
    }

    @Test
    public void testRealType()
    {
        MaterializedResult result = computeActual("SELECT price FROM " + JSON_TABLE + " WHERE vendor = 'vendor1'");
        assertEquals(getOnlyElement(result.getTypes()), REAL);
        assertEquals(result.getOnlyValue(), 3.5F);
    }

    @Test
    public void testIntegerType()
    {
        assertThat(query("SELECT lucky_number FROM " + JSON_TABLE + " WHERE vendor = 'vendor1'"))
                .matches("VALUES (INTEGER '5')")
                .isFullyPushedDown();
    }

    @Test
    public void testBrokerColumnMappingForSelectQueries()
    {
        String expected = "VALUES" +
                "  ('3.5', 'vendor1')," +
                "  ('4.5', 'vendor2')," +
                "  ('5.5', 'vendor3')," +
                "  ('6.5', 'vendor4')," +
                "  ('7.5', 'vendor5')," +
                "  ('8.5', 'vendor6')";
        assertQuery("SELECT price, vendor FROM \"SELECT price, vendor FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'\"", expected);
        assertQuery("SELECT price, vendor FROM \"SELECT * FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'\"", expected);
        assertQuery("SELECT price, vendor FROM \"SELECT vendor, lucky_numbers, price FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'\"", expected);
    }

    @Test
    public void testBrokerColumnMappingsForQueriesWithAggregates()
    {
        String passthroughQuery = "\"SELECT city, COUNT(*), MAX(price), SUM(lucky_number) " +
                "  FROM " + JSON_TABLE +
                "  WHERE vendor != 'vendor7'" +
                "  GROUP BY city\"";
        assertQuery("SELECT * FROM " + passthroughQuery, "VALUES" +
                "  ('New York', 2, 6.5, 14)," +
                "  ('Los Angeles', 4, 8.5, 31)");
        assertQuery("SELECT \"max(price)\", city, \"sum(lucky_number)\", \"count(*)\" FROM " + passthroughQuery, "VALUES" +
                "  (6.5, 'New York', 14, 2)," +
                "  (8.5, 'Los Angeles', 31, 4)");
        assertQuery("SELECT \"max(price)\", city, \"count(*)\" FROM " + passthroughQuery, "VALUES" +
                "  (6.5, 'New York', 2)," +
                "  (8.5, 'Los Angeles', 4)");
    }

    @Test
    public void testBrokerColumnMappingsForArrays()
    {
        assertQuery("SELECT ARRAY_MIN(unlucky_numbers), ARRAY_MAX(long_numbers), ELEMENT_AT(neighbors, 2), ARRAY_MIN(lucky_numbers), ARRAY_MAX(prices)" +
                        "  FROM \"SELECT unlucky_numbers, long_numbers, neighbors, lucky_numbers, prices" +
                        "  FROM " + JSON_TABLE +
                        "  WHERE vendor = 'vendor1'\"",
                "VALUES (-3.7, 20000000, 'bar1', 5, 5.5)");
        assertQuery("SELECT CARDINALITY(unlucky_numbers), CARDINALITY(long_numbers), CARDINALITY(neighbors), CARDINALITY(lucky_numbers), CARDINALITY(prices)" +
                        "  FROM \"SELECT unlucky_numbers, long_numbers, neighbors, lucky_numbers, prices" +
                        "  FROM " + JSON_TABLE +
                        "  WHERE vendor = 'vendor1'\"",
                "VALUES (3, 3, 3, 3, 2)");
    }

    @Test
    public void testCountStarQueries()
    {
        assertQuery("SELECT COUNT(*) FROM \"SELECT * FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'\"", "VALUES(6)");
        assertQuery("SELECT COUNT(*) FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'", "VALUES(6)");
        assertQuery("SELECT \"count(*)\" FROM \"SELECT COUNT(*) FROM " + JSON_TABLE + " WHERE vendor != 'vendor7'\"", "VALUES(6)");
    }

    @Test
    public void testBrokerQueriesWithAvg()
    {
        assertQuery("SELECT city, \"avg(lucky_number)\", \"avg(price)\", \"avg(long_number)\"" +
                "  FROM \"SELECT city, AVG(price), AVG(lucky_number), AVG(long_number) FROM " + JSON_TABLE + " WHERE vendor != 'vendor7' GROUP BY city\"", "VALUES" +
                "  ('New York', 7.0, 5.5, 10000.0)," +
                "  ('Los Angeles', 7.75, 6.25, 10000.0)");
        MaterializedResult result = computeActual("SELECT \"avg(lucky_number)\"" +
                "  FROM \"SELECT AVG(lucky_number) FROM my_table WHERE vendor in ('vendor2', 'vendor4')\"");
        assertEquals(getOnlyElement(result.getTypes()), DOUBLE);
        assertEquals(result.getOnlyValue(), 7.0);
    }

    @Test
    public void testNonLowerCaseColumnNames()
    {
        String mixedCaseColumnNamesTableValues = "VALUES ('string_0', '0', '1620604800')," +
                "  ('string_1', '1', '1620604801')," +
                "  ('string_2', '2', '1620604802')," +
                "  ('string_3', '3', '1620604803')";

        // Test segment query all rows
        assertQuery("SELECT stringcol, longcol, updatedatseconds" +
                        "  FROM " + MIXED_CASE_COLUMN_NAMES_TABLE,
                mixedCaseColumnNamesTableValues);

        // Test broker query all rows
        assertQuery("SELECT stringcol, longcol, updatedatseconds" +
                        "  FROM  \"SELECT updatedatseconds, longcol, stringcol FROM " + MIXED_CASE_COLUMN_NAMES_TABLE + "\"",
                mixedCaseColumnNamesTableValues);

        String singleRowValues = "VALUES (VARCHAR 'string_3', BIGINT '3', BIGINT '1620604803')";

        // Test segment query single row
        assertThat(query("SELECT stringcol, longcol, updatedatseconds" +
                        "  FROM " + MIXED_CASE_COLUMN_NAMES_TABLE +
                        "  WHERE longcol = 3"))
                .matches(singleRowValues)
                .isFullyPushedDown();

        // Test broker query single row
        assertThat(query("SELECT stringcol, longcol, updatedatseconds" +
                        "  FROM  \"SELECT updatedatseconds, longcol, stringcol FROM " + MIXED_CASE_COLUMN_NAMES_TABLE +
                        "\" WHERE longcol = 3"))
                .matches(singleRowValues)
                .isFullyPushedDown();
    }

    @Test
    public void testLimitForSegmentQueries()
    {
        // The connector will not allow segment queries to return more than MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES.
        // This is not a pinot error, it is enforced by the connector to avoid stressing pinot servers.
        assertQueryFails("SELECT string_col, updated_at_seconds FROM " + TOO_MANY_ROWS_TABLE,
                format("Segment query returned '%2$s' rows per split, maximum allowed is '%1$s' rows. with query \"SELECT string_col, updated_at_seconds FROM too_many_rows_REALTIME  LIMIT %2$s\"", MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES, MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 1));

        // Verify the row count is greater than the max rows per segment limit
        assertQuery("SELECT \"count(*)\" FROM \"SELECT COUNT(*) FROM " + TOO_MANY_ROWS_TABLE + "\"", format("VALUES(%s)", MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 1));
    }

    @Test
    public void testBrokerQueryWithTooManyRowsForSegmentQuery()
    {
        // Note:
        // This data does not include the null row inserted in createQueryRunner().
        // This verifies that if the time column has a null value, pinot does not
        // ingest the row from kafka.
        String tooManyRowsTableValues = "VALUES ('string_0', '1620604800')," +
                "  ('string_1', '1620604801')," +
                "  ('string_2', '1620604802')," +
                "  ('string_3', '1620604803')," +
                "  ('string_4', '1620604804')," +
                "  ('string_5', '1620604805')," +
                "  ('string_6', '1620604806')," +
                "  ('string_7', '1620604807')," +
                "  ('string_8', '1620604808')," +
                "  ('string_9', '1620604809')," +
                "  ('string_10', '1620604810')," +
                "  ('string_11', '1620604811')";

        // Explicit limit is necessary otherwise pinot returns 10 rows.
        // The limit is greater than the result size returned.
        assertQuery("SELECT string_col, updated_at_seconds" +
                        "  FROM  \"SELECT updated_at_seconds, string_col FROM " + TOO_MANY_ROWS_TABLE +
                        "  LIMIT " + (MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + 2) + "\"",
                tooManyRowsTableValues);
    }

    @Test
    public void testMaxLimitForPassthroughQueries()
            throws InterruptedException
    {
        assertQueryFails("SELECT string_col, updated_at_seconds" +
                        "  FROM  \"SELECT updated_at_seconds, string_col FROM " + TOO_MANY_BROKER_ROWS_TABLE +
                        "  LIMIT " + (MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES + 1) + "\"",
                "Broker query returned '13' rows, maximum allowed is '12' rows. with query \"select updated_at_seconds, string_col from too_many_broker_rows limit 13\"");

        // Pinot issue preventing Integer.MAX_VALUE from being a limit: https://github.com/apache/incubator-pinot/issues/7110
        assertQueryFails("SELECT * FROM \"SELECT string_col, long_col FROM " + ALL_TYPES_TABLE + " LIMIT " + Integer.MAX_VALUE + "\"",
                "Unexpected response status: 500 for request \\{\"sql\" : \"select string_col, long_col from alltypes limit 2147483647\" \\} to url http://localhost:\\d+/query/sql, with headers \\{Accept=\\[application/json\\], Content-Type=\\[application/json\\]\\}, full response null");

        // Pinot broker requests do not handle limits greater than Integer.MAX_VALUE
        // Note that -2147483648 is due to an integer overflow in Pinot: https://github.com/apache/pinot/issues/7242
        assertQueryFails("SELECT * FROM \"SELECT string_col, long_col FROM " + ALL_TYPES_TABLE + " LIMIT " + ((long) Integer.MAX_VALUE + 1) + "\"",
                "Query select string_col, long_col from alltypes limit -2147483648 encountered exception org.apache.pinot.common.response.broker.QueryProcessingException@\\w+ with query \"select string_col, long_col from alltypes limit -2147483648\"");

        String tooManyBrokerRowsTableValues = "VALUES ('string_0', '1620604800')," +
                "  ('string_1', '1620604801')," +
                "  ('string_2', '1620604802')," +
                "  ('string_3', '1620604803')," +
                "  ('string_4', '1620604804')," +
                "  ('string_5', '1620604805')," +
                "  ('string_6', '1620604806')," +
                "  ('string_7', '1620604807')," +
                "  ('string_8', '1620604808')," +
                "  ('string_9', '1620604809')," +
                "  ('string_10', '1620604810')," +
                "  ('string_11', '1620604811')";

        // Explicit limit is necessary otherwise pinot returns 10 rows.
        assertQuery("SELECT string_col, updated_at_seconds" +
                        "  FROM  \"SELECT updated_at_seconds, string_col FROM " + TOO_MANY_BROKER_ROWS_TABLE +
                        "  WHERE string_col != 'string_12'" +
                        "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES + "\"",
                tooManyBrokerRowsTableValues);
    }

    @Test
    public void testCount()
    {
        assertQuery("SELECT \"count(*)\" FROM \"SELECT COUNT(*) FROM " + ALL_TYPES_TABLE + "\"", "VALUES " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES);
        // If no limit is supplied to a broker query, 10 arbitrary rows will be returned. Verify this behavior:
        MaterializedResult result = computeActual("SELECT * FROM \"SELECT bool_col FROM " + ALL_TYPES_TABLE + "\"");
        assertEquals(result.getRowCount(), DEFAULT_PINOT_LIMIT_FOR_BROKER_QUERIES);
    }

    @Test
    public void testNullBehavior()
    {
        // Verify the null behavior of pinot:

        // Default null value for long single value columns is 0
        assertThat(query("SELECT long_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(BIGINT '0')")
                .isFullyPushedDown();

        // Default null value for long array values is Long.MIN_VALUE,
        assertThat(query("SELECT element_at(long_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(BIGINT '" + Long.MIN_VALUE + "')")
                .isNotFullyPushedDown(ProjectNode.class);

        // Default null value for int single value columns is 0
        assertThat(query("SELECT int_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES(INTEGER '0')")
                .isFullyPushedDown();

        // Default null value for int array values is Integer.MIN_VALUE,
        assertThat(query("SELECT element_at(int_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES(INTEGER '" + Integer.MIN_VALUE + "')")
                .isNotFullyPushedDown(ProjectNode.class);

        // Verify a null value for an array with all null values is a single element.
        // The original value inserted from kafka is 5 null elements.
        assertThat(query("SELECT element_at(int_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(INTEGER '" + Integer.MIN_VALUE + "')")
                .isNotFullyPushedDown(ProjectNode.class);

        // Verify default null value for array matches expected result
        assertThat(query("SELECT element_at(int_array_col_with_pinot_default, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES(INTEGER '7')")
                .isNotFullyPushedDown(ProjectNode.class);

        // Verify an array with null and non-null values omits the null values
        assertThat(query("SELECT int_array_col_with_pinot_default" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(CAST(ARRAY[-1112, 753, -9238] AS ARRAY(INTEGER)))")
                .isFullyPushedDown();

        // Default null value for strings is the string 'null'
        assertThat(query("SELECT string_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE bytes_col = X'' AND element_at(bool_array_col, 1) = 'null'"))
                .matches("VALUES (VARCHAR 'null')")
                .isNotFullyPushedDown(FilterNode.class);

        // Default array null value for strings is the string 'null'
        assertThat(query("SELECT element_at(string_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE bytes_col = X'' AND element_at(bool_array_col, 1) = 'null'"))
                .matches("VALUES (VARCHAR 'null')")
                .isNotFullyPushedDown(ExchangeNode.class, ProjectNode.class, FilterNode.class);

        // Default null value for booleans is the string 'null'
        // Booleans are treated as a string
        assertThat(query("SELECT bool_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES (VARCHAR 'null')")
                .isFullyPushedDown();

        // Default array null value for booleans is the string 'null'
        // Boolean are treated as a string
        assertThat(query("SELECT element_at(bool_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES (VARCHAR 'null')")
                .isNotFullyPushedDown(ProjectNode.class);

        // Default null value for pinot BYTES type (varbinary) is the string 'null'
        // BYTES values are treated as a strings
        // BYTES arrays are not supported
        assertThat(query("SELECT bytes_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES (VARBINARY '')")
                .isFullyPushedDown();

        // Default null value for float single value columns is 0.0F
        assertThat(query("SELECT float_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(REAL '0.0')")
                .isFullyPushedDown();

        // Default null value for float array values is -INFINITY,
        assertThat(query("SELECT element_at(float_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(CAST(-POWER(0, -1) AS REAL))")
                .isNotFullyPushedDown(ProjectNode.class);

        // Default null value for double single value columns is 0.0D
        assertThat(query("SELECT double_col" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(DOUBLE '0.0')")
                .isFullyPushedDown();

        // Default null value for double array values is -INFINITY,
        assertThat(query("SELECT element_at(double_array_col, 1)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES(-POWER(0, -1))")
                .isNotFullyPushedDown(ProjectNode.class);

        // Null behavior for arrays:
        // Default value for a "null" array is 1 element with default null array value,
        // Values are tested above, this test is to verify pinot returns an array with 1 element.
        assertThat(query("SELECT CARDINALITY(string_array_col)," +
                        "  CARDINALITY(bool_array_col)," +
                        "  CARDINALITY(int_array_col_with_pinot_default)," +
                        "  CARDINALITY(int_array_col)," +
                        "  CARDINALITY(float_array_col)," +
                        "  CARDINALITY(long_array_col)," +
                        "  CARDINALITY(long_array_col)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'null'"))
                .matches("VALUES (BIGINT '1', BIGINT '1', BIGINT '1', BIGINT '1', BIGINT '1', BIGINT '1', BIGINT '1')")
                .isNotFullyPushedDown(ProjectNode.class);

        // If an array contains both null and non-null values, the null values are omitted:
        // There are 5 values in the avro records, but only the 3 non-null values are in pinot
        assertThat(query("SELECT CARDINALITY(string_array_col)," +
                        "  CARDINALITY(bool_array_col)," +
                        "  CARDINALITY(int_array_col_with_pinot_default)," +
                        "  CARDINALITY(int_array_col)," +
                        "  CARDINALITY(float_array_col)," +
                        "  CARDINALITY(long_array_col)," +
                        "  CARDINALITY(long_array_col)" +
                        "  FROM " + ALL_TYPES_TABLE +
                        "  WHERE string_col = 'array_null'"))
                .matches("VALUES (BIGINT '3', BIGINT '3', BIGINT '3', BIGINT '1', BIGINT '1', BIGINT '1', BIGINT '1')")
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    public void testBrokerQueriesWithCaseStatementsInFilter()
    {
        // Need to invoke the UPPER function since identifiers are lower case
        assertQuery("SELECT city, \"avg(lucky_number)\", \"avg(price)\", \"avg(long_number)\"" +
                "  FROM \"SELECT city, AVG(price), AVG(lucky_number), AVG(long_number) FROM my_table WHERE " +
                "  CASE WHEN city = CONCAT(CONCAT(UPPER('N'), 'ew ', ''), CONCAT(UPPER('Y'), 'ork', ''), '') THEN city WHEN city = CONCAT(CONCAT(UPPER('L'), 'os ', ''), CONCAT(UPPER('A'), 'ngeles', ''), '') THEN city ELSE 'gotham' END != 'gotham'" +
                "  AND CASE WHEN vendor = 'vendor1' THEN 'vendor1' WHEN vendor = 'vendor2' THEN 'vendor2' ELSE vendor END != 'vendor7' GROUP BY city\"", "VALUES" +
                "  ('New York', 7.0, 5.5, 10000.0)," +
                "  ('Los Angeles', 7.75, 6.25, 10000.0)");
    }

    @Test
    public void testFilterWithRealLiteral()
    {
        String expectedSingleValue = "VALUES (REAL '3.5', VARCHAR 'vendor1')";
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price = 3.5")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price <= 3.5")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price BETWEEN 3 AND 4")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price > 3 AND price < 4")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price >= 3.5 AND price <= 4")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price < 3.6")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price IN (3.5)")).matches(expectedSingleValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price IN (3.5, 4)")).matches(expectedSingleValue).isFullyPushedDown();
        // NOT IN is not pushed down
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price NOT IN (4.5, 5.5, 6.5, 7.5, 8.5, 9.5)")).isNotFullyPushedDown(FilterNode.class);

        String expectedMultipleValues = "VALUES" +
                "  (REAL '3.5', VARCHAR 'vendor1')," +
                "  (REAL '4.5', VARCHAR 'vendor2')";
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price < 4.6")).matches(expectedMultipleValues).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price BETWEEN 3.5 AND 4.5")).matches(expectedMultipleValues).isFullyPushedDown();

        String expectedMaxValue = "VALUES (REAL '9.5', VARCHAR 'vendor7')";
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price > 9")).matches(expectedMaxValue).isFullyPushedDown();
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE price >= 9")).matches(expectedMaxValue).isFullyPushedDown();
    }

    @Test
    public void testArrayFilter()
    {
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE vendor != 'vendor7' AND prices = ARRAY[3.5, 5.5]"))
                .matches("VALUES (REAL '3.5', VARCHAR 'vendor1')")
                .isNotFullyPushedDown(FilterNode.class);

        // Array filters are not pushed down, as there are no array literals in pinot
        assertThat(query("SELECT price, vendor FROM " + JSON_TABLE + " WHERE prices = ARRAY[3.5, 5.5]")).isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT string_col, long_col FROM " + "\"SELECT string_col, long_col, bool_col FROM " + ALL_TYPES_TABLE + " WHERE int_col > 0\" " +
                "  WHERE bool_col = 'false' LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();
        assertThat(query("SELECT string_col, long_col FROM " + ALL_TYPES_TABLE + "  WHERE int_col >0 AND bool_col = 'false' LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isNotFullyPushedDown(LimitNode.class);
    }

    @Test
    public void testCreateTable()
    {
        assertQueryFails("CREATE TABLE test_create_table (col INT)", "This connector does not support creating tables");
    }

    /**
     * https://github.com/trinodb/trino/issues/8307
     */
    @Test
    public void testInformationSchemaColumnsTableNotExist()
    {
        assertThat(query("SELECT * FROM pinot.information_schema.columns WHERE table_name = 'table_not_exist'"))
                .returnsEmptyResult();
    }

    @Test
    public void testAggregationPushdown()
    {
        // Without the limit inside the passthrough query, pinot will only return 10 rows
        assertThat(query("SELECT COUNT(*) FROM \"SELECT * FROM " + ALL_TYPES_TABLE + " LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + "\""))
                .isFullyPushedDown();

        // Test aggregates with no grouping columns
        assertThat(query("SELECT COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)" +
                "  FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();

        // Test aggregates with no grouping columns with a limit
        assertThat(query("SELECT COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();

        // Test aggregates with no grouping columns with a filter
        assertThat(query("SELECT COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)" +
                "  FROM " + ALL_TYPES_TABLE + " WHERE long_col < 4147483649"))
                .isFullyPushedDown();

        // Test aggregates with no grouping columns with a filter and limit
        assertThat(query("SELECT COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)" +
                "  FROM " + ALL_TYPES_TABLE + " WHERE long_col < 4147483649" +
                "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();

        // Test aggregates with one grouping column
        assertThat(query("SELECT bool_col, COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();

        // Test aggregates with one grouping column and a limit
        assertThat(query("SELECT string_col, COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY string_col" +
                "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();

        // Test aggregates with one grouping column and a filter
        assertThat(query("SELECT bool_col, COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)" +
                "  FROM " + ALL_TYPES_TABLE + " WHERE long_col < 4147483649 GROUP BY bool_col"))
                .isFullyPushedDown();

        // Test aggregates with one grouping column, a filter and a limit
        assertThat(query("SELECT string_col, COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)" +
                "  FROM " + ALL_TYPES_TABLE + " WHERE long_col < 4147483649 GROUP BY string_col" +
                "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();

        // Test single row from pinot where filter results in an empty result set.
        // A direct pinot query would return 1 row with default values, not null values.
        assertThat(query("SELECT COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)" +
                "  FROM " + ALL_TYPES_TABLE + " WHERE long_col > 4147483649")).isFullyPushedDown();

        // Test passthrough queries with no aggregates
        assertThat(query("SELECT string_col, COUNT(*)," +
                "  MIN(int_col), MAX(int_col)," +
                "  MIN(long_col), MAX(long_col), AVG(long_col), SUM(long_col)," +
                "  MIN(float_col), MAX(float_col), AVG(float_col), SUM(float_col)," +
                "  MIN(double_col), MAX(double_col), AVG(double_col), SUM(double_col)" +
                "  FROM \"SELECT * FROM " + ALL_TYPES_TABLE + " WHERE long_col > 4147483649" +
                "  LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES + "\"  GROUP BY string_col"))
                .isFullyPushedDown();

        // Passthrough queries with aggregates will not push down more aggregations.
        assertThat(query("SELECT bool_col, \"count(*)\", COUNT(*) FROM \"SELECT bool_col, count(*) FROM " +
                ALL_TYPES_TABLE + " GROUP BY bool_col\" GROUP BY bool_col, \"count(*)\""))
                .isNotFullyPushedDown(ExchangeNode.class, ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        assertThat(query("SELECT bool_col, \"max(long_col)\", COUNT(*) FROM \"SELECT bool_col, max(long_col) FROM " +
                ALL_TYPES_TABLE + " GROUP BY bool_col\" GROUP BY bool_col, \"max(long_col)\""))
                .isNotFullyPushedDown(ExchangeNode.class, ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        assertThat(query("SELECT int_col, COUNT(*) FROM " + ALL_TYPES_TABLE + " GROUP BY int_col LIMIT " + MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                .isFullyPushedDown();

        // count(<column>) should not be pushed down, as pinot currently only implements count(*)
        assertThat(query("SELECT bool_col, COUNT(long_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // AVG on INTEGER columns is not pushed down
        assertThat(query("SELECT string_col, AVG(int_col) FROM " + ALL_TYPES_TABLE + " GROUP BY string_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // SUM on INTEGER columns is not pushed down
        assertThat(query("SELECT string_col, SUM(int_col) FROM " + ALL_TYPES_TABLE + " GROUP BY string_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // MIN on VARCHAR columns is not pushed down
        assertThat(query("SELECT bool_col, MIN(string_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // MAX on VARCHAR columns is not pushed down
        assertThat(query("SELECT bool_col, MAX(string_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // COUNT on VARCHAR columns is not pushed down
        assertThat(query("SELECT bool_col, COUNT(string_col)" +
                "  FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // Distinct on varchar is pushed down
        assertThat(query("SELECT DISTINCT string_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on bool is pushed down
        assertThat(query("SELECT DISTINCT bool_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on double is pushed down
        assertThat(query("SELECT DISTINCT double_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on float is pushed down
        assertThat(query("SELECT DISTINCT float_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on long is pushed down
        assertThat(query("SELECT DISTINCT long_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Distinct on int is partially pushed down
        assertThat(query("SELECT DISTINCT int_col FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(ExchangeNode.class);

        // Distinct on 2 columns for supported types:
        assertThat(query("SELECT DISTINCT bool_col, string_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT DISTINCT bool_col, double_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT DISTINCT bool_col, float_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT DISTINCT bool_col, long_col FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT DISTINCT bool_col, int_col FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(ExchangeNode.class);

        // Test distinct for mixed case values
        assertThat(query("SELECT DISTINCT string_col FROM " + MIXED_CASE_DISTINCT_TABLE))
                .isFullyPushedDown();

        // Test count distinct for mixed case values
        assertThat(query("SELECT COUNT(DISTINCT string_col) FROM " + MIXED_CASE_DISTINCT_TABLE))
                .isFullyPushedDown();

        // Approx distinct for mixed case values
        assertThat(query("SELECT approx_distinct(string_col) FROM " + MIXED_CASE_DISTINCT_TABLE))
                .isFullyPushedDown();

        // Approx distinct on varchar is pushed down
        assertThat(query("SELECT approx_distinct(string_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Approx distinct on bool is pushed down
        assertThat(query("SELECT approx_distinct(bool_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Approx distinct on double is pushed down
        assertThat(query("SELECT approx_distinct(double_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Approx distinct on float is pushed down
        assertThat(query("SELECT approx_distinct(float_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Approx distinct on long is pushed down
        assertThat(query("SELECT approx_distinct(long_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        // Approx distinct on int is partially pushed down
        assertThat(query("SELECT approx_distinct(int_col) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(ExchangeNode.class);

        // Approx distinct on 2 columns for supported types:
        assertThat(query("SELECT bool_col, approx_distinct(string_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, approx_distinct(double_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, approx_distinct(float_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, approx_distinct(long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, approx_distinct(int_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ExchangeNode.class);

        // Distinct count is fully pushed down by default
        assertThat(query("SELECT bool_col, COUNT(DISTINCT string_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, COUNT(DISTINCT double_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, COUNT(DISTINCT float_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        assertThat(query("SELECT bool_col, COUNT(DISTINCT int_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isFullyPushedDown();
        // Test queries with no grouping columns
        assertThat(query("SELECT COUNT(DISTINCT string_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(DISTINCT bool_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(DISTINCT double_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(DISTINCT float_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(DISTINCT int_col) FROM " + ALL_TYPES_TABLE))
                .isFullyPushedDown();

        // Aggregation is not pushed down for queries with count distinct and other aggregations
        assertThat(query("SELECT bool_col, MAX(long_col), COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT bool_col, COUNT(DISTINCT long_col), MAX(long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT bool_col, COUNT(*), COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT bool_col, COUNT(DISTINCT long_col), COUNT(*) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        // Test queries with no grouping columns
        assertThat(query("SELECT MAX(long_col), COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT COUNT(DISTINCT long_col), MAX(long_col) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT COUNT(*), COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        assertThat(query("SELECT COUNT(DISTINCT long_col), COUNT(*) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);

        Session countDistinctPushdownDisabledSession = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty("pinot", "count_distinct_pushdown_enabled", "false")
                .build();

        // Distinct count is partially pushed down when the distinct_count_pushdown_enabled session property is disabled
        assertThat(query(countDistinctPushdownDisabledSession, "SELECT bool_col, COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col"))
                .isNotFullyPushedDown(ExchangeNode.class, ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);
        // Test query with no grouping columns
        assertThat(query(countDistinctPushdownDisabledSession, "SELECT COUNT(DISTINCT long_col) FROM " + ALL_TYPES_TABLE))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class);

        // Ensure that count(<column name>) is not pushed down even when a broker query is present
        // This is also done as the second step of count distinct but should not be pushed down in this case.
        assertThat(query("SELECT COUNT(long_col) FROM \"SELECT long_col FROM " + ALL_TYPES_TABLE + "\""))
                .isNotFullyPushedDown(AggregationNode.class);

        // Ensure that count(<column name>) is not pushed down even when a broker query is present and has grouping columns
        // This is also done as the second step of count distinct but should not be pushed down in this case.
        assertThat(query("SELECT bool_col, COUNT(long_col) FROM \"SELECT bool_col, long_col FROM " + ALL_TYPES_TABLE + "\" GROUP BY bool_col"))
                .isNotFullyPushedDown(ExchangeNode.class, ProjectNode.class, AggregationNode.class, ExchangeNode.class, ExchangeNode.class, AggregationNode.class, ProjectNode.class);

        // Ensure that count(<column name>) is not pushed down even if the query contains a matching grouping column
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> query("SELECT COUNT(long_col) FROM \"SELECT long_col FROM " + ALL_TYPES_TABLE + " GROUP BY long_col\""))
                .withRootCauseInstanceOf(RuntimeException.class)
                .withMessage("Operation not supported for DISTINCT aggregation function");

        // Ensure that count(<column name>) with grouping columns is not pushed down even if the query contains a matching grouping column
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> query("SELECT bool_col, COUNT(long_col) FROM \"SELECT bool_col, long_col FROM " + ALL_TYPES_TABLE + " GROUP BY bool_col, long_col\""))
                .withRootCauseInstanceOf(RuntimeException.class)
                .withMessage("Operation not supported for DISTINCT aggregation function");
    }

    @Test
    public void testInClause()
    {
        assertThat(query("SELECT string_col, sum(long_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col IN ('string_1200','string_2400','string_3600')" +
                "  GROUP BY string_col"))
                .isFullyPushedDown();

        assertThat(query("SELECT string_col, sum(long_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col NOT IN ('string_1200','string_2400','string_3600')" +
                "  GROUP BY string_col"))
                .isFullyPushedDown();

        assertThat(query("SELECT int_col, sum(long_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE int_col IN (54, 56)" +
                "  GROUP BY int_col"))
                .isFullyPushedDown();

        assertThat(query("SELECT int_col, sum(long_col)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE int_col NOT IN (54, 56)" +
                "  GROUP BY int_col"))
                .isFullyPushedDown();
    }

    @Test
    public void testVarbinaryFilters()
    {
        assertThat(query("SELECT string_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col = X''"))
                .matches("VALUES (VARCHAR 'null'), (VARCHAR 'array_null')")
                .isFullyPushedDown();

        assertThat(query("SELECT string_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col != X''"))
                .matches("VALUES (VARCHAR 'string_0')," +
                        "  (VARCHAR 'string_1200')," +
                        "  (VARCHAR 'string_2400')," +
                        "  (VARCHAR 'string_3600')," +
                        "  (VARCHAR 'string_4800')," +
                        "  (VARCHAR 'string_6000')," +
                        "  (VARCHAR 'string_7200')," +
                        "  (VARCHAR 'string_8400')," +
                        "  (VARCHAR 'string_9600')")
                .isFullyPushedDown();

        assertThat(query("SELECT string_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col = X'73 74 72 69 6e 67 5f 30'"))
                .matches("VALUES (VARCHAR 'string_0')")
                .isFullyPushedDown();

        assertThat(query("SELECT string_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col != X'73 74 72 69 6e 67 5f 30'"))
                .matches("VALUES (VARCHAR 'null')," +
                        "  (VARCHAR 'array_null')," +
                        "  (VARCHAR 'string_1200')," +
                        "  (VARCHAR 'string_2400')," +
                        "  (VARCHAR 'string_3600')," +
                        "  (VARCHAR 'string_4800')," +
                        "  (VARCHAR 'string_6000')," +
                        "  (VARCHAR 'string_7200')," +
                        "  (VARCHAR 'string_8400')," +
                        "  (VARCHAR 'string_9600')")
                .isFullyPushedDown();
    }

    @Test
    public void testRealWithInfinity()
    {
        assertThat(query("SELECT element_at(float_array_col, 1)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col = X''"))
                .matches("VALUES  (CAST(-POWER(0, -1) AS REAL))," +
                        "  (CAST(-POWER(0, -1) AS REAL))");

        assertThat(query("SELECT element_at(float_array_col, 1) FROM \"SELECT float_array_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE bytes_col = '' \""))
                .matches("VALUES  (CAST(-POWER(0, -1) AS REAL))," +
                        "  (CAST(-POWER(0, -1) AS REAL))");

        assertThat(query("SELECT element_at(float_array_col, 2)" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col = 'string_0'"))
                .matches("VALUES (CAST(POWER(0, -1) AS REAL))");

        assertThat(query("SELECT element_at(float_array_col, 2) FROM \"SELECT float_array_col" +
                "  FROM " + ALL_TYPES_TABLE +
                "  WHERE string_col = 'string_0'\""))
                .matches("VALUES (CAST(POWER(0, -1) AS REAL))");
    }
}
