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

import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.String.format;

public class TestIpAddressDecoder
        extends AbstractDecoderTest
{
    @Override
    protected String getParseFailureErrorMsg(Object value)
    {
        return null;
    }

    @Override
    protected String getTypeMismatchErrorMsg(Object value)
    {
        return format("Expected a string value for field '%s' of type IP: %s [%s]", PATH, value, value.getClass().getSimpleName());
    }

    @BeforeClass
    public void setUp()
    {
        decoder = new IpAddressDecoder(IPADDRESS);
        init();
    }

    @Test
    public void testIntegerValue()
    {
        // positive integer
        Integer value = 101;
        convertAndAssertTypeMismatchThrowable(value);

        // negative integer
        value = -1 * value;
        convertAndAssertTypeMismatchThrowable(value);
    }

    @Test
    public void testLongValue()
    {
        // positive long
        Long value = 123456789123456789L;
        convertAndAssertTypeMismatchThrowable(value);

        // negative long
        value = -1 * value;
        convertAndAssertTypeMismatchThrowable(value);
    }

    @Test
    public void testDoubleValue()
    {
        // positive double
        Double value = 123.123;
        convertAndAssertTypeMismatchThrowable(value);

        // negative double
        value = -1 * value;
        convertAndAssertTypeMismatchThrowable(value);
    }

    @Test
    public void testFloatValue()
    {
        // positive float
        Float value = 123.123f;
        convertAndAssertTypeMismatchThrowable(value);

        // negative float
        value = -1 * value;
        convertAndAssertTypeMismatchThrowable(value);
    }

    /*
    TODO: Add unit tests for IpAddressDecoder
     */
}
