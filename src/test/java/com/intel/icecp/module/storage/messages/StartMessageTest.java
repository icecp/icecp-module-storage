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

import com.intel.icecp.core.Channel;
import com.intel.icecp.core.Node;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.core.metadata.Persistence;
import com.intel.icecp.core.misc.ChannelIOException;
import com.intel.icecp.core.misc.ChannelLifetimeException;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.ack.AckMessage;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * StartMessage unit tests.
 *
 */
public class StartMessageTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    StorageModule mockModule;
    @Mock
    StorageProvider mockProvider;
    @Mock
    Node mockNode;
    @Mock
    Channel<BytesMessage> mockChannel;
    @Mock
    Channel<AckMessage> ackChannel;
    StartMessage msg;

    @Before
    public void beforeClass() throws URISyntaxException {
        MockitoAnnotations.initMocks(this);
        final int maxBufPeriodInSecForTest = 10;
        msg = new StartMessage("uri://listenChannel", maxBufPeriodInSecForTest);
    }

    @Test
    public void testConstructorListenChannel() {
        String testUri = "uri://testChannel";
        StartMessage startMsg = new StartMessage(testUri);
        assertEquals(testUri, startMsg.getListenChannel());
    }

    @Test
    public void testConstructorMaxBufferingPeriodPositive() {
        String testUri = "uri://testChannel";
        int maxBufferingPeriodForTest = 5;
        StartMessage startMsg = new StartMessage(testUri, maxBufferingPeriodForTest);
        assertEquals(maxBufferingPeriodForTest, startMsg.getMaxBufferingPeriodInSec());
    }

    @Test
    public void testConstructorMaxBufferingPeriodNegative() {
        String testUri = "uri://testChannel";
        int maxBufferingPeriodForTest = -100;
        StartMessage startMsg = new StartMessage(testUri, maxBufferingPeriodForTest);
        assertNotEquals(maxBufferingPeriodForTest, startMsg.getMaxBufferingPeriodInSec());
        assertTrue(startMsg.getMaxBufferingPeriodInSec() == StorageModule.DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC);
    }

    @Test
    public void throwWhenChannelOpenFails() throws Exception {
        createMockModule(14);
        //mock out an exception when trying to open a channel
        Mockito.<Channel<BytesMessage>>when(mockNode.openChannel(any(URI.class), any(), any(Persistence.class))).thenThrow(new ChannelLifetimeException("mock message"));

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);

        //Assert we got back a response, it was an error response, and subscribe was not called (because channel open failed)
        Mockito.verify(mockChannel, times(0)).subscribe(any());
    }

    @Test
    public void throwWhenChannelSubscribeFails() throws Exception {
        createMockModule(15);
        Mockito.<Channel<BytesMessage>>when(mockNode.openChannel(any(URI.class), any(), any(Persistence.class))).thenReturn(mockChannel);
        //when the subscribe it attempted, and exception will be thrown
        doThrow(new ChannelIOException("mockException")).when(mockChannel).subscribe(any());

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);

        //Assert we got back a response, and it was an error response, and it came from the subscribe
        Mockito.verify(mockChannel, times(1)).subscribe(any());
    }

    @Test
    public void throwWhenListenChannelIsNull() throws StorageModuleException {
        msg = new StartMessage(null);
        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void startSessionSuccessfully() throws Exception {
        Long sessionId = (long) 200;
        createMockModule(sessionId);
        Mockito.<Channel<BytesMessage>>when(mockNode.openChannel(any(URI.class), any(), any(Persistence.class))).thenReturn(mockChannel);

        Object resp = msg.onCommandMessage(mockModule);

        assertEquals(sessionId, resp);
    }

    @Test
    public void getNonEmptyToStringInformation() {
        String testUri = "uri://testChannel";
        int maxBufferingPeriodForTest = -100;
        StartMessage startMsg = new StartMessage(testUri, maxBufferingPeriodForTest);
        String toStringInfo = startMsg.toString();

        assertNotNull(toStringInfo);
        assertNotEquals("", toStringInfo);
        assertTrue(toStringInfo.contains("listenChannel"));
        assertTrue(toStringInfo.contains("maxBufferingPeriodInSec"));
    }

    private void createMockModule(long mockSessionId) throws Exception {
        when(mockProvider.createSession(any(), anyInt())).thenReturn(mockSessionId);
        when(mockModule.getNode()).thenReturn(mockNode);
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        when(mockModule.getAckChannel()).thenReturn(ackChannel);
    }
}