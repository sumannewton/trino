package io.trino.plugin.elasticsearch.decoders;

import io.trino.spi.TrinoException;
import io.trino.testing.assertions.Assert;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestBigintDecoder
        extends AbstractDecoderTest
{
    @Override
    protected AbstractDecoder createDecoder()
    {
        return new BigintDecoder();
    }

    @Override
    protected Class<?> outputType()
    {
        return Long.class;
    }

    @Override
    protected boolean supportsBoolean()
    {
        return false;
    }

    @Test
    public void testPositiveString() {
        final String string_value = "112233";
        when(valueSupplier.get()).thenReturn(string_value);
        if (!this.supportsString()) {
            assertThatThrownBy(() -> getDecoder().decode("test_column", valueSupplier, output)).isInstanceOf(TrinoException.class);
        }
        final Object actual = getDecoder().convert("test_column", valueSupplier.get());
        assertEquals(actual, Long.valueOf(string_value));
        getDecoder().decode("test_column", valueSupplier, output);
    }

    @Test
    public void testNegativeString() {
        final String string_value = "-112233";
        when(valueSupplier.get()).thenReturn(string_value);

        final Object actual = getDecoder().convert("test_column", valueSupplier.get());
        assertEquals(actual, Long.valueOf(string_value));
        getDecoder().decode("test_column", valueSupplier, output);
    }

    @Test
    public void testEmptyString() {
        final Object expected = "";
        when(valueSupplier.get()).thenReturn(expected);

        assertThatThrownBy(() -> getDecoder().convert("test_column", valueSupplier.get()))
                .hasMessage("Cannot parse value for field 'test_column' as BIGINT: ")
                .isInstanceOf(TrinoException.class);
    }
}
