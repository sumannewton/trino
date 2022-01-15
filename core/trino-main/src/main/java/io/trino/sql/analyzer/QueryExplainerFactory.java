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
package io.trino.sql.analyzer;

import io.trino.cost.CostCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.metadata.Metadata;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanOptimizersFactory;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class QueryExplainerFactory
{
    private final PlanOptimizersFactory planOptimizersFactory;
    private final PlanFragmenter planFragmenter;
    private final Metadata metadata;
    private final TypeOperators typeOperators;
    private final SqlParser sqlParser;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    @Inject
    public QueryExplainerFactory(
            PlanOptimizersFactory planOptimizersFactory,
            PlanFragmenter planFragmenter,
            Metadata metadata,
            TypeOperators typeOperators,
            SqlParser sqlParser,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator)
    {
        this.planOptimizersFactory = requireNonNull(planOptimizersFactory, "planOptimizersFactory is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    public QueryExplainer createQueryExplainer(AnalyzerFactory analyzerFactory)
    {
        return new QueryExplainer(
                planOptimizersFactory,
                planFragmenter,
                metadata,
                typeOperators,
                sqlParser,
                analyzerFactory,
                statsCalculator,
                costCalculator);
    }
}
