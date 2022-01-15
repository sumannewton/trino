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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Pattern;

import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_DELETE;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A connector smoke test exercising various connector functionalities without going in depth on any of them.
 * A connector should implement {@link BaseConnectorTest} and use this class to exercise some configuration variants.
 */
public abstract class BaseConnectorSmokeTest
        extends AbstractTestQueryFramework
{
    protected static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(NATION, REGION);

    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return connectorBehavior.hasBehaviorByDefault(this::hasBehavior);
    }

    /**
     * Ensure the tests are run with {@link DistributedQueryRunner}. E.g. {@link LocalQueryRunner} takes some
     * shortcuts, not exercising certain aspects.
     */
    @Test
    public void ensureDistributedQueryRunner()
    {
        assertThat(getQueryRunner().getNodeCount()).as("query runner node count")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    @Override
    public void ensureTestNamingConvention()
    {
        // Enforce a naming convention to make code navigation easier.
        assertThat(getClass().getName())
                .endsWith("ConnectorSmokeTest");
    }

    @Test
    public void testSelect()
    {
        assertQuery("SELECT name FROM region");
    }

    @Test
    public void testPredicate()
    {
        assertQuery("SELECT name, regionkey FROM nation WHERE nationkey = 10");
        assertQuery("SELECT name, regionkey FROM nation WHERE nationkey BETWEEN 5 AND 15");
        assertQuery("SELECT name, regionkey FROM nation WHERE name = 'EGYPT'");
    }

    @Test
    public void testLimit()
    {
        assertQuery("SELECT name FROM region LIMIT 5");
    }

    @Test
    public void testTopN()
    {
        assertQuery("SELECT regionkey FROM nation ORDER BY name LIMIT 3");
    }

    @Test
    public void testAggregation()
    {
        assertQuery("SELECT sum(regionkey) FROM nation");
        assertQuery("SELECT sum(nationkey) FROM nation GROUP BY regionkey");
    }

    @Test
    public void testHaving()
    {
        assertQuery("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 58", "VALUES (4, 58)");
    }

    @Test
    public void testJoin()
    {
        assertQuery("SELECT n.name, r.name FROM nation n JOIN region r on n.regionkey = r.regionkey");
    }

    @Test
    public void testCreateTable()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            assertQueryFails("CREATE TABLE xxxx (a bigint, b double)", "This connector does not support creating tables");
            return;
        }

        String tableName = "test_create_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double)");
        assertThat(query("SELECT a, b FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableAsSelect()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            assertQueryFails("CREATE TABLE xxxx AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", "This connector does not support creating tables with data");
            return;
        }

        String tableName = "test_create_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsert()
    {
        if (!hasBehavior(SUPPORTS_INSERT)) {
            assertQueryFails("INSERT INTO region (regionkey) VALUES (42)", "This connector does not support inserts");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test INSERT without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        String tableName = "test_create_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double)");
        assertUpdate("INSERT INTO " + tableName + " (a, b) VALUES (42, -38.5)", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDelete()
    {
        if (!hasBehavior(SUPPORTS_DELETE)) {
            assertQueryFails("DELETE FROM region", "This connector does not support deletes");
            return;
        }

        if (!hasBehavior(SUPPORTS_ROW_LEVEL_DELETE)) {
            assertQueryFails("DELETE FROM region WHERE regionkey = 2", ".*[Dd]elet(e|ing).*(not |un)supported.*");
            return;
        }

        String tableName = "test_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM region", 5);

        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 2", 1);
        assertThat(query("SELECT * FROM " + tableName + " WHERE regionkey = 2"))
                .returnsEmptyResult();
        assertThat(query("SELECT cast(regionkey AS integer) FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES 0, 1, 3, 4");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateSchema()
    {
        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            assertQueryFails("CREATE SCHEMA xxxxxx", "This connector does not support creating schemas");
            getSession().getSchema().ifPresent(
                    s -> assertQueryFails("DROP SCHEMA " + s, "This connector does not support dropping schemas"));
            return;
        }

        String schemaName = "test_schema_create_" + randomTableSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertThat(query("SHOW SCHEMAS"))
                .skippingTypesCheck()
                .containsAll(format("VALUES '%s', '%s'", getSession().getSchema().orElseThrow(), schemaName));
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testRenameTable()
    {
        if (!hasBehavior(SUPPORTS_RENAME_TABLE)) {
            assertQueryFails("ALTER TABLE nation RENAME TO yyyy", "This connector does not support renaming tables");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test ALTER TABLE RENAME without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        String oldTable = "test_rename_old_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + oldTable + " (a bigint, b double)");

        String newTable = "test_rename_new_" + randomTableSuffix();
        assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);

        assertThat(query("SHOW TABLES LIKE '" + oldTable + "'"))
                .returnsEmptyResult();
        assertThat(query("SELECT a, b FROM " + newTable))
                .returnsEmptyResult();

        if (hasBehavior(SUPPORTS_INSERT)) {
            assertUpdate("INSERT INTO " + newTable + " (a, b) VALUES (42, -38.5)", 1);
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + newTable))
                    .matches("VALUES (BIGINT '42', -385e-1)");
        }

        assertUpdate("DROP TABLE " + newTable);
        assertThat(query("SHOW TABLES LIKE '" + newTable + "'"))
                .returnsEmptyResult();
    }

    @Test
    public void testRenameTableAcrossSchemas()
    {
        if (!hasBehavior(SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS)) {
            if (!hasBehavior(SUPPORTS_RENAME_TABLE)) {
                throw new SkipException("Skipping since rename table is not supported at all");
            }
            assertQueryFails("ALTER TABLE nation RENAME TO other_schema.yyyy", "This connector does not support renaming tables across schemas");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            throw new AssertionError("Cannot test ALTER TABLE RENAME across schemas without CREATE SCHEMA, the test needs to be implemented in a connector-specific way");
        }

        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test ALTER TABLE RENAME across schemas without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        String oldTable = "test_rename_old_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + oldTable + " (a bigint, b double)");

        String schemaName = "test_schema_" + randomTableSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);

        String newTable = schemaName + ".test_rename_new_" + randomTableSuffix();
        assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);

        assertThat(query("SHOW TABLES LIKE '" + oldTable + "'"))
                .returnsEmptyResult();
        assertThat(query("SELECT a, b FROM " + newTable))
                .returnsEmptyResult();

        if (hasBehavior(SUPPORTS_INSERT)) {
            assertUpdate("INSERT INTO " + newTable + " (a, b) VALUES (42, -38.5)", 1);
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + newTable))
                    .matches("VALUES (BIGINT '42', -385e-1)");
        }

        assertUpdate("DROP TABLE " + newTable);
        assertThat(query("SHOW TABLES LIKE '" + newTable + "'"))
                .returnsEmptyResult();

        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testSelectInformationSchemaTables()
    {
        assertThat(query(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", getSession().getSchema().orElseThrow())))
                .skippingTypesCheck()
                .containsAll("VALUES 'nation', 'region'");
    }

    @Test
    public void testSelectInformationSchemaColumns()
    {
        assertThat(query(format("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = 'region'", getSession().getSchema().orElseThrow())))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment'");
    }

    // SHOW CREATE TABLE exercises table properties and comments, which may be skipped during regular SELECT execution
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches(format(
                        "CREATE TABLE %s.%s.region \\(\n" +
                                "   regionkey (bigint|decimal\\(19, 0\\)),\n" +
                                "   name varchar(\\(\\d+\\))?,\n" +
                                "   comment varchar(\\(\\d+\\))?\n" +
                                "\\)",
                        Pattern.quote(getSession().getCatalog().orElseThrow()),
                        Pattern.quote(getSession().getSchema().orElseThrow())));
    }

    @Test
    public void testView()
    {
        if (!hasBehavior(SUPPORTS_CREATE_VIEW)) {
            assertQueryFails("CREATE VIEW nation_v AS SELECT * FROM nation", "This connector does not support creating views");
            return;
        }

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String viewName = "test_view_" + randomTableSuffix();
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM nation");

        assertThat(query("SELECT * FROM " + viewName))
                .skippingTypesCheck()
                .matches("SELECT * FROM nation");

        assertThat(((String) computeScalar("SHOW CREATE VIEW " + viewName)))
                .matches("(?s)" +
                        "CREATE VIEW \\Q" + catalogName + "." + schemaName + "." + viewName + "\\E" +
                        ".* AS\n" +
                        "SELECT \\*\n" +
                        "FROM\n" +
                        "  nation");

        assertUpdate("DROP  VIEW " + viewName);
    }

    @Test
    public void testMaterializedView()
    {
        if (!hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW)) {
            assertQueryFails("CREATE MATERIALIZED VIEW nation_mv AS SELECT * FROM nation", "This connector does not support creating materialized views");
            return;
        }

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String viewName = "test_materialized_view_" + randomTableSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " AS SELECT * FROM nation");

        // reading
        assertThat(query("SELECT * FROM " + viewName))
                .skippingTypesCheck()
                .matches("SELECT * FROM nation");

        // details
        assertThat(((String) computeScalar("SHOW CREATE MATERIALIZED VIEW " + viewName)))
                .matches("(?s)" +
                        "CREATE MATERIALIZED VIEW \\Q" + catalogName + "." + schemaName + "." + viewName + "\\E" +
                        ".* AS\n" +
                        "SELECT \\*\n" +
                        "FROM\n" +
                        "  nation");

        assertUpdate("DROP MATERIALIZED VIEW " + viewName);
    }
}
