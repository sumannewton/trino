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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.BoundSignature;
import io.trino.operator.PagesIndex;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;

import static io.trino.operator.aggregation.AccumulatorCompiler.generateAccumulatorFactoryBinder;
import static java.util.Objects.requireNonNull;

public final class InternalAggregationFunction
{
    private final List<Type> parameterTypes;
    private final List<Class<?>> lambdaInterfaces;
    private final AccumulatorFactoryBinder factory;

    public InternalAggregationFunction(BoundSignature boundSignature, AggregationMetadata aggregationMetadata)
    {
        requireNonNull(boundSignature, "boundSignature is null");
        requireNonNull(aggregationMetadata, "aggregationMetadata is null");
        this.parameterTypes = boundSignature.getArgumentTypes();
        this.factory = generateAccumulatorFactoryBinder(aggregationMetadata);
        this.lambdaInterfaces = ImmutableList.copyOf(aggregationMetadata.getLambdaInterfaces());
    }

    @VisibleForTesting
    public List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    public List<Class<?>> getLambdaInterfaces()
    {
        return lambdaInterfaces;
    }

    public AccumulatorFactory bind(List<Integer> inputChannels, Optional<Integer> maskChannel)
    {
        return factory.bind(
                inputChannels,
                maskChannel,
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                null,
                false,
                null,
                null,
                ImmutableList.of(),
                null);
    }

    public AccumulatorFactory bind(
            List<Integer> inputChannels,
            Optional<Integer> maskChannel,
            List<Type> sourceTypes,
            List<Integer> orderByChannels,
            List<SortOrder> orderings,
            PagesIndex.Factory pagesIndexFactory,
            boolean distinct,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            List<LambdaProvider> lambdaProviders,
            Session session)
    {
        return factory.bind(inputChannels, maskChannel, sourceTypes, orderByChannels, orderings, pagesIndexFactory, distinct, joinCompiler, blockTypeOperators, lambdaProviders, session);
    }
}
