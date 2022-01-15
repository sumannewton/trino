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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.AggregationFromAnnotationsParser;
import io.trino.operator.aggregation.InternalAggregationFunction;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public abstract class SqlAggregationFunction
        implements SqlFunction
{
    private final FunctionMetadata functionMetadata;
    private final boolean orderSensitive;
    private final boolean decomposable;

    public static List<SqlAggregationFunction> createFunctionsByAnnotations(Class<?> aggregationDefinition)
    {
        return ImmutableList.copyOf(AggregationFromAnnotationsParser.parseFunctionDefinitions(aggregationDefinition));
    }

    protected SqlAggregationFunction(FunctionMetadata functionMetadata, boolean decomposable, boolean orderSensitive)
    {
        this.functionMetadata = requireNonNull(functionMetadata, "functionMetadata is null");
        checkArgument(functionMetadata.isDeterministic(), "Aggregation function must be deterministic");
        this.orderSensitive = orderSensitive;
        this.decomposable = decomposable;
    }

    @Override
    public FunctionMetadata getFunctionMetadata()
    {
        return functionMetadata;
    }

    public AggregationFunctionMetadata getAggregationMetadata(FunctionBinding functionBinding)
    {
        if (!decomposable) {
            return new AggregationFunctionMetadata(orderSensitive, Optional.empty());
        }

        List<TypeSignature> intermediateTypes = getIntermediateTypes(functionBinding);
        TypeSignature intermediateType;
        if (intermediateTypes.size() == 1) {
            intermediateType = getOnlyElement(intermediateTypes);
        }
        else {
            intermediateType = new TypeSignature(StandardTypes.ROW, intermediateTypes.stream()
                    .map(TypeSignatureParameter::anonymousField)
                    .collect(toImmutableList()));
        }
        return new AggregationFunctionMetadata(orderSensitive, Optional.of(intermediateType));
    }

    protected List<TypeSignature> getIntermediateTypes(FunctionBinding functionBinding)
    {
        throw new UnsupportedOperationException();
    }

    public InternalAggregationFunction specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        return specialize(functionBinding);
    }

    protected InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        throw new UnsupportedOperationException();
    }
}
