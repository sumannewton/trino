package io.trino.plugin.elasticsearch.decoders;

import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.function.Supplier;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public abstract class AbstractDecoderTest
{
    public static final String PATH = "test_column";
    public BlockBuilder output;
    public Supplier<Object> valueSupplier;
    private AbstractDecoder decoder;

    protected abstract AbstractDecoder createDecoder();

    protected abstract Class<?> outputType();

    protected boolean supportsString()
    {
        return true;
    }

    protected boolean supportsBoolean()
    {
        return true;
    }

    protected boolean supportsNumber()
    {
        return true;
    }

    protected final AbstractDecoder getDecoder()
    {
        return decoder;
    }

    @BeforeClass
    public void init()
    {
        output = mock(BlockBuilder.class);
        valueSupplier = mock(Supplier.class);
        decoder = spy(createDecoder());

        when(output.appendNull()).thenReturn(output);
        doNothing().when(decoder).write(any(BlockBuilder.class), any(Object.class));
    }

    @Test
    public void testNullValue() {
        when(valueSupplier.get()).thenReturn(null);
        decoder.decode("test_column", valueSupplier, output);
    }

    @Test
    public void testIntegerValue() {
        final Object number_value = 101;
        when(valueSupplier.get()).thenReturn(number_value);
        if (!supportsNumber()) {
            assertThatThrownBy(() -> decoder.decode(PATH, valueSupplier, output))
                    .hasMessage("Expected a boolean value for field %s of type %s: %s [Integer]", PATH, decoder.getType().toString().toUpperCase(), number_value)
                    .isInstanceOf(TrinoException.class);
        }
        else {
            assertInstanceOf(decoder.convert("test_column", valueSupplier.get()), outputType());
            decoder.decode("test_column", valueSupplier, output);
        }
    }

    @Test
    public void testBooleanValue() {
        when(valueSupplier.get()).thenReturn(true);
        if (!supportsBoolean()) {
            assertThatThrownBy(() -> decoder.decode("test_column", valueSupplier, output)).isInstanceOf(TrinoException.class);
        }
        else {
            assertInstanceOf(decoder.convert("test_column", valueSupplier.get()), outputType());
            decoder.decode("test_column", valueSupplier, output);
        }

        when(valueSupplier.get()).thenReturn(false);
        if (!supportsBoolean()) {
            assertThatThrownBy(() -> decoder.decode("test_column", valueSupplier, output)).isInstanceOf(TrinoException.class);
        }
        else {
            assertInstanceOf(decoder.convert("test_column", valueSupplier.get()), outputType());
            decoder.decode("test_column", valueSupplier, output);
        }
    }

//    protected abstract String getErrorMessage();
}
