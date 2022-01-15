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
package io.trino.plugin.ml;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.trino.RowPageBuilder;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.Accumulator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.metadata.FunctionExtractor.extractFunctions;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;

public class TestEvaluateClassifierPredictions
{
    @Test
    public void testEvaluateClassifierPredictions()
    {
        TestingFunctionResolution functionResolution = new TestingFunctionResolution()
                .addFunctions(extractFunctions(new MLPlugin().getFunctions()));
        TestingAggregationFunction aggregation = functionResolution.getAggregateFunction(
                QualifiedName.of("evaluate_classifier_predictions"),
                fromTypes(BIGINT, BIGINT));
        Accumulator accumulator = aggregation.bind(ImmutableList.of(0, 1), Optional.empty()).createAccumulator();
        accumulator.addInput(getPage());
        BlockBuilder finalOut = accumulator.getFinalType().createBlockBuilder(null, 1);
        accumulator.evaluateFinal(finalOut);
        Block block = finalOut.build();

        String output = VARCHAR.getSlice(block, 0).toStringUtf8();
        List<String> parts = ImmutableList.copyOf(Splitter.on('\n').omitEmptyStrings().split(output));
        assertEquals(parts.size(), 7, output);
        assertEquals(parts.get(0), "Accuracy: 1/2 (50.00%)");
    }

    private static Page getPage()
    {
        return RowPageBuilder.rowPageBuilder(BIGINT, BIGINT)
                .row(1L, 1L)
                .row(1L, 0L)
                .build();
    }
}
