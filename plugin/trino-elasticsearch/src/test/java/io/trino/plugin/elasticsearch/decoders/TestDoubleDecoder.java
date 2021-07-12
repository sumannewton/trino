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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.String.format;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestDoubleDecoder
        extends AbstractDecoderTest
{
    @Override
    protected String getParseFailureErrorMsg(Object value)
    {
        return format("Cannot parse value for field '%s' as TINYINT: %s", PATH, value);
    }

    @Override
    protected String getTypeMismatchErrorMsg(Object value)
    {
        return format("Expected a numeric value for field '%s' of type TINYINT: %s [%s]", PATH, value, value.getClass().getSimpleName());
    }

    @BeforeClass
    public void setUp()
    {
        decoder = new TinyintDecoder();
        init();
    }

    @Test
    public void testIntegerValue()
    {
        // positive integer
        Integer value = 101;
        when(valueSupplier.get()).thenReturn(value);
        assertEquals(decoder.convert(PATH, valueSupplier.get()), Long.valueOf(value.longValue()));

        // negative integer
        value = -1 * value;
        when(valueSupplier.get()).thenReturn(value);
        assertEquals(decoder.convert(PATH, valueSupplier.get()), Long.valueOf(value.longValue()));
    }

    @Test
    public void testOutOfRangeIntegerValue()
    {
        // positive integer
        Integer value = 123456;
        when(valueSupplier.get()).thenReturn(value);
        assertTrinoExceptionThrownBy(() -> decoder.decode(PATH, valueSupplier, output),
                format("Value out of range for field '%s' of type TINYINT: %s", PATH, value));

        // negative integer
        value = -1 * value;
        when(valueSupplier.get()).thenReturn(value);
        assertTrinoExceptionThrownBy(() -> decoder.decode(PATH, valueSupplier, output),
                format("Value out of range for field '%s' of type TINYINT: %s", PATH, value));
    }

    @Test
    public void testLongValue()
    {
        // positive long
        Long value = 123L;
        when(valueSupplier.get()).thenReturn(value);
        assertEquals(decoder.convert(PATH, valueSupplier.get()), value);

        // negative long
        value = -1 * value;
        when(valueSupplier.get()).thenReturn(value);
        assertEquals(decoder.convert(PATH, valueSupplier.get()), value);
    }

    @Test
    public void testOutOfRangeLongValue()
    {
        // positive long
        Long value = 123456789123456789L;
        when(valueSupplier.get()).thenReturn(value);
        assertTrinoExceptionThrownBy(() -> decoder.decode(PATH, valueSupplier, output),
                format("Value out of range for field '%s' of type TINYINT: %s", PATH, value));

        // negative long
        value = -1 * value;
        when(valueSupplier.get()).thenReturn(value);
        assertTrinoExceptionThrownBy(() -> decoder.decode(PATH, valueSupplier, output),
                format("Value out of range for field '%s' of type TINYINT: %s", PATH, value));
    }

    @Test
    public void testDoubleValue()
    {
        // positive double
        Double value = 123.123;
        when(valueSupplier.get()).thenReturn(value);
        assertEquals(decoder.convert(PATH, valueSupplier.get()), Long.valueOf(value.longValue()));

        // negative double
        value = -1 * value;
        when(valueSupplier.get()).thenReturn(value);
        assertEquals(decoder.convert(PATH, valueSupplier.get()), Long.valueOf(value.longValue()));
    }

    @Test
    public void testOutOfRangeDoubleValue()
    {
        // positive double
        Double value = 123456.123;
        when(valueSupplier.get()).thenReturn(value);
        assertTrinoExceptionThrownBy(() -> decoder.decode(PATH, valueSupplier, output),
                format("Value out of range for field '%s' of type TINYINT: %s", PATH, value.longValue()));

        // negative double
        value = -1 * value;
        when(valueSupplier.get()).thenReturn(value);
        assertTrinoExceptionThrownBy(() -> decoder.decode(PATH, valueSupplier, output),
                format("Value out of range for field '%s' of type TINYINT: %s", PATH, value.longValue()));
    }

    @Test
    public void testFloatValue()
    {
        // positive float
        Float value = 123.123f;
        when(valueSupplier.get()).thenReturn(value);
        assertEquals(decoder.convert(PATH, valueSupplier.get()), Long.valueOf(value.longValue()));

        // negative float
        value = -1 * value;
        when(valueSupplier.get()).thenReturn(value);
        assertEquals(decoder.convert(PATH, valueSupplier.get()), Long.valueOf(value.longValue()));
    }

    @Test
    public void testOutOfRangeFloatValue()
    {
        // positive float
        Float value = 123456.123f;
        when(valueSupplier.get()).thenReturn(value);
        assertTrinoExceptionThrownBy(() -> decoder.decode(PATH, valueSupplier, output),
                format("Value out of range for field '%s' of type TINYINT: %s", PATH, value.longValue()));

        // negative float
        value = -1 * value;
        when(valueSupplier.get()).thenReturn(value);
        assertTrinoExceptionThrownBy(() -> decoder.decode(PATH, valueSupplier, output),
                format("Value out of range for field '%s' of type TINYINT: %s", PATH, value.longValue()));
    }

    @Test
    public void testInvalidStringValue()
    {
        convertAndAssertParseErrorThrowable("invalid");
        convertAndAssertParseErrorThrowable("");
    }

    @Test
    public void testBooleanValue()
    {
        convertAndAssertTypeMismatchThrowable(true);
        convertAndAssertTypeMismatchThrowable(false);
    }
}
