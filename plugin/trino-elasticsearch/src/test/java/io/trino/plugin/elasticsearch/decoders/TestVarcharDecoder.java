package io.trino.plugin.elasticsearch.decoders;

import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

public class TestVarcharDecoder
        extends AbstractDecoderTest
{
    @Override
    protected AbstractDecoder createDecoder()
    {
        return new VarcharDecoder();
    }

    @Override
    protected Class<?> outputType()
    {
        return String.class;
    }

    @Test
    public void testValidString() {
        final Object string_value = "string_value";
        when(valueSupplier.get()).thenReturn(string_value);
        if (!this.supportsString()) {
            assertThatThrownBy(() -> getDecoder().decode("test_column", valueSupplier, output)).isInstanceOf(TrinoException.class);
        }
        assertInstanceOf(getDecoder().convert("test_column", valueSupplier.get()), outputType());
        getDecoder().decode("test_column", valueSupplier, output);
    }

    @Test
    public void testEmptyString() {
        final Object string_value = "";
        when(valueSupplier.get()).thenReturn(string_value);
        if (!this.supportsString()) {
            assertThatThrownBy(() -> getDecoder().decode("test_column", valueSupplier, output)).isInstanceOf(TrinoException.class);
        }
        assertInstanceOf(getDecoder().convert("test_column", valueSupplier.get()), outputType());
        getDecoder().decode("test_column", valueSupplier, output);
    }
}
