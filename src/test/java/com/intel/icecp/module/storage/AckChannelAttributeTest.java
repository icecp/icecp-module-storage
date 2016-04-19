/*
 * ******************************************************************************
 *
 * INTEL CONFIDENTIAL
 *
 * Copyright 2013 - 2016 Intel Corporation All Rights Reserved.
 *
 * The source code contained or described herein and all documents related to
 * the source code ("Material") are owned by Intel Corporation or its suppliers
 * or licensors. Title to the Material remains with Intel Corporation or its
 * suppliers and licensors. The Material contains trade secrets and proprietary
 * and confidential information of Intel or its suppliers and licensors. The
 * Material is protected by worldwide copyright and trade secret laws and treaty
 * provisions. No part of the Material may be used, copied, reproduced,
 * modified, published, uploaded, posted, transmitted, distributed, or disclosed
 * in any way without Intel's prior express written permission.
 *
 * No license under any patent, copyright, trade secret or other intellectual
 * property right is granted to or conferred upon you by disclosure or delivery
 * of the Materials, either expressly, by implication, inducement, estoppel or
 * otherwise. Any license under such intellectual property rights must be
 * express and approved by Intel in writing.
 *
 * Unless otherwise agreed by Intel in writing, you may not remove or alter this
 * notice or any other notice embedded in Materials by Intel or Intel's
 * suppliers or licensors in any way.
 *
 * ******************************************************************************
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