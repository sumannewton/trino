package io.trino.plugin.elasticsearch.decoders;

import io.trino.spi.TrinoException;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

public class TestBooleanDecoder
        extends AbstractDecoderTest
{
    @Override
    protected AbstractDecoder createDecoder()
    {
        return new BooleanDecoder();
    }

    @Override
    protected Class<?> outputType()
    {
        return Boolean.class;
    }

    @Override
    protected boolean supportsNumber()
    {
        return true;
    }

    @Test
    public void testValidStringTrue() {
        final Object value = "true";
        when(valueSupplier.get()).thenReturn(value);
        assertInstanceOf(getDecoder().convert("test_column", valueSupplier.get()), outputType());
        getDecoder().decode("test_column", valueSupplier, output);
    }

    @Test
    public void testValidStringFalse() {
        final Object value = "false";
        when(valueSupplier.get()).thenReturn(value);
        assertInstanceOf(getDecoder().convert("test_column", valueSupplier.get()), outputType());
        getDecoder().decode("test_column", valueSupplier, output);
    }

    @Test
    public void testInvalidString() {
        final Object value = "invalid";
        when(valueSupplier.get()).thenReturn(value);
        assertThatThrownBy(() -> getDecoder().decode("test_column", valueSupplier, output))
                .hasMessage(getErrorMessage(), value)
                .isInstanceOf(TrinoException.class);
    }

//    @Override
    protected String getErrorMessage()
    {
        return "Cannot parse value for field 'test_column' as BOOLEAN: [%s]";
    }
}
