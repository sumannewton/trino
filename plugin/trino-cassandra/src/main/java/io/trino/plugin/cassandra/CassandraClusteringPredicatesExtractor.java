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
package io.trino.plugin.cassandra;

import com.datastax.driver.core.VersionNumber;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.cassandra.util.CassandraCqlUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CassandraClusteringPredicatesExtractor
{
    private final ClusteringPushDownResult clusteringPushDownResult;
    private final TupleDomain<ColumnHandle> predicates;

    public CassandraClusteringPredicatesExtractor(List<CassandraColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates, VersionNumber cassandraVersion)
    {
        this.predicates = requireNonNull(predicates, "predicates is null");
        this.clusteringPushDownResult = getClusteringKeysSet(clusteringColumns, predicates, requireNonNull(cassandraVersion, "cassandraVersion is null"));
    }

    public String getClusteringKeyPredicates()
    {
        return clusteringPushDownResult.getDomainQuery();
    }

    public TupleDomain<ColumnHandle> getUnenforcedConstraints()
    {
        return predicates.filter(((columnHandle, domain) -> !clusteringPushDownResult.hasBeenFullyPushed(columnHandle)));
    }

    private static ClusteringPushDownResult getClusteringKeysSet(List<CassandraColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates, VersionNumber cassandraVersion)
    {
        ImmutableSet.Builder<ColumnHandle> fullyPushedColumnPredicates = ImmutableSet.builder();
        ImmutableList.Builder<String> clusteringColumnSql = ImmutableList.builder();
        int currentClusteringColumn = 0;
        for (CassandraColumnHandle columnHandle : clusteringColumns) {
            Domain domain = predicates.getDomains().get().get(columnHandle);
            if (domain == null) {
                break;
            }
            if (domain.isNullAllowed()) {
                break;
            }
            String predicateString = domain.getValues().getValuesProcessor().transform(
                    ranges -> {
                        List<Object> singleValues = new ArrayList<>();
                        List<String> rangeConjuncts = new ArrayList<>();
                        String predicate = null;

                        for (Range range : ranges.getOrderedRanges()) {
                            if (range.isAll()) {
                                return null;
                            }
                            if (range.isSingleValue()) {
                                singleValues.add(columnHandle.getCassandraType().toCqlLiteral(range.getSingleValue()));
                            }
                            else {
                                if (!range.isLowUnbounded()) {
                                    String lowBound = columnHandle.getCassandraType().toCqlLiteral(range.getLowBoundedValue());
                                    rangeConjuncts.add(format(
                                            "%s %s %s",
                                            CassandraCqlUtils.validColumnName(columnHandle.getName()),
                                            range.isLowInclusive() ? ">=" : ">",
                                            lowBound));
                                }
                                if (!range.isHighUnbounded()) {
                                    String highBound = columnHandle.getCassandraType().toCqlLiteral(range.getHighBoundedValue());
                                    rangeConjuncts.add(format(
                                            "%s %s %s",
                                            CassandraCqlUtils.validColumnName(columnHandle.getName()),
                                            range.isHighInclusive() ? "<=" : "<",
                                            highBound));
                                }
                            }
                        }

                        if (!singleValues.isEmpty() && !rangeConjuncts.isEmpty()) {
                            return null;
                        }
                        if (!singleValues.isEmpty()) {
                            if (singleValues.size() == 1) {
                                predicate = CassandraCqlUtils.validColumnName(columnHandle.getName()) + " = " + singleValues.get(0);
                            }
                            else {
                                predicate = CassandraCqlUtils.validColumnName(columnHandle.getName()) + " IN ("
                                        + Joiner.on(",").join(singleValues) + ")";
                            }
                        }
                        else if (!rangeConjuncts.isEmpty()) {
                            predicate = Joiner.on(" AND ").join(rangeConjuncts);
                        }
                        return predicate;
                    }, discreteValues -> {
                        if (discreteValues.isInclusive()) {
                            ImmutableList.Builder<Object> discreteValuesList = ImmutableList.builder();
                            for (Object discreteValue : discreteValues.getValues()) {
                                discreteValuesList.add(columnHandle.getCassandraType().toCqlLiteral(discreteValue));
                            }
                            String predicate = CassandraCqlUtils.validColumnName(columnHandle.getName()) + " IN ("
                                    + Joiner.on(",").join(discreteValuesList.build()) + ")";
                            return predicate;
                        }
                        return null;
                    }, allOrNone -> null);

            if (predicateString == null) {
                break;
            }
            // IN restriction only on last clustering column for Cassandra version = 2.1
            if (predicateString.contains(" IN (") && cassandraVersion.compareTo(VersionNumber.parse("2.2.0")) < 0 && currentClusteringColumn != (clusteringColumns.size() - 1)) {
                break;
            }
            clusteringColumnSql.add(predicateString);
            fullyPushedColumnPredicates.add(columnHandle);
            // Check for last clustering column should only be restricted by range condition
            if (predicateString.contains(">") || predicateString.contains("<")) {
                break;
            }
            currentClusteringColumn++;
        }
        List<String> clusteringColumnPredicates = clusteringColumnSql.build();

        return new ClusteringPushDownResult(fullyPushedColumnPredicates.build(), Joiner.on(" AND ").join(clusteringColumnPredicates));
    }

    private static class ClusteringPushDownResult
    {
        private final Set<ColumnHandle> fullyPushedColumnPredicates;
        private final String domainQuery;

        public ClusteringPushDownResult(Set<ColumnHandle> fullyPushedColumnPredicates, String domainQuery)
        {
            this.fullyPushedColumnPredicates = ImmutableSet.copyOf(requireNonNull(fullyPushedColumnPredicates, "fullyPushedColumnPredicates is null"));
            this.domainQuery = requireNonNull(domainQuery);
        }

        public boolean hasBeenFullyPushed(ColumnHandle column)
        {
            return fullyPushedColumnPredicates.contains(column);
        }

        public String getDomainQuery()
        {
            return domainQuery;
        }
    }
}
