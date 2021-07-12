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
package io.trino.plugin.elasticsearch.decoders;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.block.BlockBuilder;
import org.testng.annotations.Test;

import java.util.function.Supplier;

import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public abstract class AbstractDecoderTest
{
    protected static final String PATH = "test_column";
    protected BlockBuilder output;
    protected Supplier<Object> valueSupplier;
    protected AbstractDecoder decoder;

    protected abstract String getParseFailureErrorMsg(Object value);

    protected abstract String getTypeMismatchErrorMsg(Object value);

    protected final void init()
    {
        output = mock(BlockBuilder.class);
        valueSupplier = mock(Supplier.class);
        decoder = spy(decoder);
        when(output.appendNull()).thenReturn(output);
        doNothing().when(decoder).write(any(BlockBuilder.class), any());
    }

    @Test
    public void testNullValue()
    {
        when(valueSupplier.get()).thenReturn(null);
        decoder.decode("test_column", valueSupplier, output);
    }

    @Test
    public void testListValue()
    {
        convertAndAssertTypeMismatchThrowable(ImmutableList.of("val1", "val2"));
    }

    @Test
    public void testMapValue()
    {
        convertAndAssertTypeMismatchThrowable(ImmutableMap.of("key1", "val1"));
    }

    protected void convertAndAssertTypeMismatchThrowable(Object value)
    {
        when(valueSupplier.get()).thenReturn(value);
        assertTrinoExceptionThrownBy(() -> decoder.decode(PATH, valueSupplier, output), getTypeMismatchErrorMsg(value));
    }

    protected void convertAndAssertParseErrorThrowable(Object value)
    {
        when(valueSupplier.get()).thenReturn(value);
        assertTrinoExceptionThrownBy(() -> decoder.decode(PATH, valueSupplier, output), getParseFailureErrorMsg(value));
    }
}
