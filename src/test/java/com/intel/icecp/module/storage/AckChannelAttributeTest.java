/*
 * Copyright (c) 2016 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// test is in this module to ensure AckChannelAttribute is visible from other packages, rather than *.attributes
package com.intel.icecp.module.storage;

import com.intel.icecp.module.storage.attributes.AckChannelAttribute;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class AckChannelAttributeTest {
    @Test
    public void testWithValidStringUri() throws URISyntaxException {
        final String uriString = "http://www.google.com/";
        final URI expected = new URI(uriString);
        URI actual = new AckChannelAttribute(uriString).value();

        assertEquals(expected, actual);
    }

    @Test
    public void testWithValidUriUri() throws URISyntaxException {
        final String uriString = "http://www.google.com/";
        final URI expected = new URI(uriString);
        URI actual = new AckChannelAttribute(expected).value();

        assertEquals(expected, actual);
    }

    @Test
    public void testWithNullUri() {
        // testing for no exception
        new AckChannelAttribute((String) null);
        new AckChannelAttribute((URI) null);
    }

    @Test(expected = AssertionError.class)
    public void testWithInvalidUri() throws URISyntaxException {
        final String uriString = "a#b#c#//q#r#s";
        new AckChannelAttribute(uriString).value();
    }
}