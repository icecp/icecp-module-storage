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
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.ack.AckMessage;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import com.intel.icecp.node.NodeFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class PersistCallbackTest {

    private static final byte[] sampleBytes = {(byte) 0x00, (byte) 0x01, (byte) 0x02, (byte) 0x03};
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private final Long SESSION_ID = 77L;
    private final Long MESSAGE_ID = 111L;
    @Mock
    StorageModule mockModule;
    @Mock
    StorageProvider mockProvider;
    @Mock
    Node mockNode;
    @Mock
    Channel<AckMessage> mockAckChannel;
    private Node node;
    private Channel<AckMessage> ackMessageChannel;
    private URI incomingChannelUri;
    private PersistCallback mockCallback, callback;
    private URI ackUri;

    @Before
    public void before() throws Exception {
        MockitoAnnotations.initMocks(this);
        ackUri = URI.create("ndn:/ack");
        incomingChannelUri = URI.create("ndn:/incoming");
        node = NodeFactory.buildMockNode();
        ackMessageChannel = node.openChannel(ackUri, AckMessage.class, Persistence.DEFAULT);

        mockCallback = new PersistCallback(mockNode, mockProvider, ackMessageChannel, incomingChannelUri, SESSION_ID);
        callback = new PersistCallback(node, mockProvider, ackMessageChannel, incomingChannelUri, SESSION_ID);
    }

    @Test
    public void persistCallbackConstructor() {
        assertEquals(SESSION_ID, mockCallback.getSessionId());
        assertNotNull(mockCallback.getDigest());
    }

    @Test
    public void testIfMessageSavedAckMessageGetsPublished() throws Exception {
        when(mockProvider.saveMessage(anyLong(), any(PersistentMessage.class))).thenReturn(MESSAGE_ID);
        CountDownLatch latch = new CountDownLatch(1);
        Channel<AckMessage> ackChannel = node.openChannel(ackUri, AckMessage.class, Persistence.DEFAULT);
        ackChannel.subscribe(m -> latch.countDown());

        mockCallback.onPublish(new BytesMessage(sampleBytes));

        latch.await(4, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void testIfMessageNotSavedAckMessageDoesNotGetPublished() throws Exception {
        when(mockProvider.saveMessage(anyLong(), any(PersistentMessage.class))).thenThrow(StorageModuleException.class);

        mockCallback.onPublish(new BytesMessage(sampleBytes));

        verify(mockAckChannel, times(0)).publish(any(AckMessage.class));
    }

    @Test
    public void testIfMessageNotSavedOpenAckChannelIsNotCalled() throws Exception {
        when(mockProvider.saveMessage(anyLong(), any(PersistentMessage.class))).thenThrow(StorageModuleException.class);

        mockCallback.onPublish(new BytesMessage(sampleBytes));

        verify(mockNode, times(0)).openChannel(any(URI.class), (Class<AckMessage>) any(), any(Persistence.class));
    }

    @Test
    public void testIfMultipleMessagesAreSavedTwoAckMessagesArePublished() throws Exception {
        when(mockProvider.saveMessage(anyLong(), any(PersistentMessage.class))).thenReturn(MESSAGE_ID);
        CountDownLatch latch = new CountDownLatch(2);
        Channel<AckMessage> ackChannel = node.openChannel(ackUri, AckMessage.class, Persistence.DEFAULT);
        ackChannel.subscribe(m -> latch.countDown());

        mockCallback.onPublish(new BytesMessage(sampleBytes));
        // publish message again
        mockCallback.onPublish(new BytesMessage(sampleBytes));

        latch.await(4, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void testCorrectAckMessageIdGotPublishedIfMessageIsSaved() throws Exception {
        when(mockProvider.saveMessage(anyLong(), any(PersistentMessage.class))).thenReturn(1L);

        BytesMessage message = new BytesMessage(sampleBytes);
        AckMessage ackMessage = new AckMessage(incomingChannelUri, getSampleStorageId());
        callback.onPublish(message);

        assertEquals(ackMessage.getId(), ackMessageChannel.latest().get().getId());
    }

    @Test
    public void testCorrectAckMessageUriGotPublishedIfMessageIsSaved() throws Exception {
        when(mockProvider.saveMessage(anyLong(), any(PersistentMessage.class))).thenReturn(MESSAGE_ID);

        BytesMessage message = new BytesMessage(sampleBytes);
        AckMessage ackMessage = new AckMessage(incomingChannelUri, getSampleStorageId());
        callback.onPublish(message);

        assertEquals(ackMessage.getUri(), ackMessageChannel.latest().get().getUri());
    }

    @Test
    public void onPublishHandlesEmptyMessageSuccessfully() throws Exception {
        when(mockNode.openChannel(any(URI.class), (Class<AckMessage>) any(), any(Persistence.class))).thenReturn(mockAckChannel);
        when(mockProvider.saveMessage(anyLong(), any(PersistentMessage.class))).thenReturn(MESSAGE_ID);

        mockCallback.onPublish(new BytesMessage(new byte[0]));

        //assert this empty message is stored successfully
        verify(mockProvider, times(1)).saveMessage(anyLong(), any(PersistentMessage.class));
    }

    @Test
    public void onPublishHandlesMessageSuccessfully() throws Exception {
        when(mockNode.openChannel(any(URI.class), (Class<AckMessage>) any(), any(Persistence.class))).thenReturn(mockAckChannel);
        when(mockProvider.saveMessage(anyLong(), any(PersistentMessage.class))).thenReturn(MESSAGE_ID);

        mockCallback.onPublish(new BytesMessage(sampleBytes));

        //assert this message is stored successfully
        verify(mockProvider, times(1)).saveMessage(anyLong(), any(PersistentMessage.class));
    }

    @Test
    public void updatesSessionIdSuccessfully() {
        Long newSessionId = 90L;

        mockCallback.setSessionId(newSessionId);

        assertEquals(newSessionId, mockCallback.getSessionId());

    }

    private long getSampleStorageId() {
        return Arrays.hashCode(new byte[]{(byte) 5, (byte) 78, (byte) -34, (byte) -63,
                (byte) -48, (byte) 33, (byte) 31, (byte) 98,
                (byte) 79, (byte) -19, (byte) 12, (byte) -68,
                (byte) -87, (byte) -44, (byte) -7, (byte) 64,
                (byte) 11, (byte) 14, (byte) 73, (byte) 28,
                (byte) 67, (byte) 116, (byte) 42, (byte) -14,
                (byte) -59, (byte) -80, (byte) -85, (byte) -21,
                (byte) -16, (byte) -55, (byte) -112, (byte) -40});
    }

}