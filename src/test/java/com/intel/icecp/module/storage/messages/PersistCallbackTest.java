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