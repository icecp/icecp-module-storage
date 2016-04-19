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

package com.intel.icecp.module.storage.messages;


import com.intel.icecp.core.Node;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Unit tests for GET_TIME_SPAN command
 *
 */
public class GetTimeSpanTest {
    private static final String QUERY_CHANNEL = "ndn:/intel/test/listen";
    @Mock
    private StorageModule mockModule;
    @Mock
    private StorageProvider mockProvider;
    @Mock
    private Node mockNode;

    private GetTimeSpan msg;
    private URI channel;

    @Before
    public void beforeTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        msg = new GetTimeSpan(QUERY_CHANNEL);
        channel = new URI(QUERY_CHANNEL);
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
    }

    @Test
    public void testConstructorListenChannel() {
        assertEquals(QUERY_CHANNEL, msg.getQueryChannel());
    }

    @Test(expected = StorageModuleException.class)
    public void throwWhenUriIsBad() throws Exception {
        (new GetTimeSpan("foo:\\bad.uri")).onCommandMessage(mockModule);
    }

    @Test
    public void testResponseComesBackSuccessfully() throws Exception {
        when(mockProvider.getActiveMinimumTimestamp(QUERY_CHANNEL)).thenReturn(0L);
        when(mockProvider.getActiveMaximumTimestamp(QUERY_CHANNEL)).thenReturn(0L);

        Object resp;
        resp = msg.onCommandMessage(mockModule);
        assertNotNull(resp);
        assertTrue(resp instanceof List);
    }

    @Test
    public void testReturnCorrectTimestampsWhenChannelHasMessages() throws Exception {
        when(mockProvider.getActiveMinimumTimestamp(QUERY_CHANNEL)).thenReturn(1L);
        when(mockProvider.getActiveMaximumTimestamp(QUERY_CHANNEL)).thenReturn(100L);

        GetTimeSpan msg = new GetTimeSpan(QUERY_CHANNEL);
        List<Long> resp = (List<Long>) msg.onCommandMessage(mockModule);

        assertEquals(resp.size(), 2);
        assertTrue(resp.contains(1L));
        assertTrue(resp.contains(100L));
    }

    @Test
    public void testSameTimeStampIsReturnedWhenMinMaxAreTheSame() throws Exception {
        when(mockProvider.getActiveMinimumTimestamp(QUERY_CHANNEL)).thenReturn(100L);
        when(mockProvider.getActiveMaximumTimestamp(QUERY_CHANNEL)).thenReturn(100L);

        GetTimeSpan msg = new GetTimeSpan(QUERY_CHANNEL);
        List<Long> resp = (List<Long>) msg.onCommandMessage(mockModule);

        assertEquals(resp.size(), 2);
        assertTrue(resp.contains(100L));
        assertTrue(resp.contains(100L));
    }
}