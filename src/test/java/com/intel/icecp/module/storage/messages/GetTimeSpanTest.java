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