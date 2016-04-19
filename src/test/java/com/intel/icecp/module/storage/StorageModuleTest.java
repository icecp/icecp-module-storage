/*
 * ******************************************************************************
 *
 *  INTEL CONFIDENTIAL
 *
 *  Copyright 2013 - 2016 Intel Corporation All Rights Reserved.
 *
 *  The source code contained or described herein and all documents related to the
 *  source code ("Material") are owned by Intel Corporation or its suppliers or
 *  licensors. Title to the Material remains with Intel Corporation or its
 *  suppliers and licensors. The Material contains trade secrets and proprietary
 *  and confidential information of Intel or its suppliers and licensors. The
 *  Material is protected by worldwide copyright and trade secret laws and treaty
 *  provisions. No part of the Material may be used, copied, reproduced, modified,
 *  published, uploaded, posted, transmitted, distributed, or disclosed in any way
 *  without Intel's prior express written permission.
 *
 *  No license under any patent, copyright, trade secret or other intellectual
 *  property right is granted to or conferred upon you by disclosure or delivery of
 *  the Materials, either expressly, by implication, inducement, estoppel or
 *  otherwise. Any license under such intellectual property rights must be express
 *  and approved by Intel in writing.
 *
 *  Unless otherwise agreed by Intel in writing, you may not remove or alter this
 *  notice or any other notice embedded in Materials by Intel or Intel's suppliers
 *  or licensors in any way.
 *
 * *********************************************************************
 */

package com.intel.icecp.module.storage;

import com.intel.icecp.core.Channel;
import com.intel.icecp.core.Message;
import com.intel.icecp.core.Module;
import com.intel.icecp.core.Node;
import com.intel.icecp.core.attributes.Attributes;
import com.intel.icecp.core.attributes.IdAttribute;
import com.intel.icecp.core.attributes.ModuleStateAttribute;
import com.intel.icecp.core.management.Channels;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.core.metadata.Persistence;
import com.intel.icecp.module.storage.messages.BaseMessage;
import com.intel.icecp.module.storage.messages.PersistCallback;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import com.intel.icecp.node.NodeFactory;
import com.intel.icecp.rpc.CommandRequest;
import com.intel.icecp.rpc.CommandResponse;
import com.intel.icecp.rpc.Rpc;
import com.intel.icecp.rpc.RpcClient;
import org.apache.commons.lang.NullArgumentException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StorageModuleTest {
    private static final long MODULE_ID = 12;
    private static final int REMOTE_CALL_TIMEOUT_MS = 10000;
    @Mock
    private Node mockNode;
    @Mock
    private Channel<Message> mockCmdChannel;
    @Mock
    private Channel<BytesMessage> mockChannel;
    @Mock
    private StorageProvider mockProvider;
    @Mock
    private BaseMessage mockMessage;
    @Mock
    private Channel<Message> mockResponseChannel;
    @Mock
    private Channels mockChannels;
    @Mock
    private PersistCallback mockPersistCallback;
    @Mock
    private Attributes mockAttributes;
    private StorageModule module;
    private URI mockUri;

    @Before
    public void before() throws Exception {
        module = new StorageModule();

        MockitoAnnotations.initMocks(this);
        when(mockNode.channels()).thenReturn(mockChannels);
        when(mockNode.channels().openChannel(any(URI.class), (Class<Message>) any(), any(Persistence.class))).thenReturn(mockResponseChannel);
        when(mockAttributes.get(eq(IdAttribute.class))).thenReturn(MODULE_ID);

        mockUri = new URI("test://testuri");
    }

    @Test
    public void constructorWhenProviderIsNull() {
        StorageModule moduleWithNullProvider = new StorageModule(null);
        assertNotNull(moduleWithNullProvider.getStorageProvider());
    }

    @Test
    public void constructorWithProvider() {
        StorageModule moduleWithProvider = new StorageModule(mockProvider);
        assertSame(mockProvider, moduleWithProvider.getStorageProvider());
    }

    @Test
    public void runWhenNodeIsNullNotifyErrorState() throws Exception {
        module.run(null, mockAttributes);

        verify(mockAttributes, times(1)).set(eq(ModuleStateAttribute.class), eq(Module.State.ERROR));
    }


    private void setNodeMockMethods() throws Exception {
        when(mockNode.getDefaultUri()).thenReturn(mockUri);
        when(mockNode.openChannel(any(URI.class), any(), any())).thenReturn(mockCmdChannel);
    }

    @Test
    public void runStartsSuccessfullyWhenParametersAreGood() throws Exception {
        setNodeMockMethods();

        module.run(mockNode, mockAttributes);

        assertSame(mockNode, module.getNode());
        verify(mockAttributes, times(1)).set(eq(ModuleStateAttribute.class), eq(Module.State.RUNNING));
    }

    @Test
    public void runOnlyOnce() throws Exception {
        setNodeMockMethods();

        module.run(mockNode, mockAttributes);

        assertSame(mockNode, module.getNode());
        verify(mockAttributes, times(1)).set(eq(ModuleStateAttribute.class), eq(Module.State.RUNNING));

        //try to run again, verify that command channel was not re-subscribed, and the state was sent out again.
        module.run(mockNode, mockAttributes);
        verify(mockAttributes, times(2)).set(eq(ModuleStateAttribute.class), eq(Module.State.RUNNING));

    }

    @Test
    public void stopModuleWithNullNodeShouldNotifyError() throws Exception {
        module.run(null, mockAttributes);
        // Any reason will do...
        module.stop(Module.StopReason.USER_DIRECTED);

        verify(mockAttributes, times(1)).set(eq(ModuleStateAttribute.class), eq(Module.State.ERROR));
    }

    @Test
    public void stopModuleShouldNotify() throws Exception {
        setNodeMockMethods();

        module.run(mockNode, mockAttributes);
        // Any reason will do...
        module.stop(Module.StopReason.USER_DIRECTED);

        verify(mockAttributes, times(1)).set(eq(ModuleStateAttribute.class), eq(Module.State.STOPPED));
    }


    @Test(expected = NullArgumentException.class)
    public void addChannelShouldFailWhenSessionIdIsNull() {
        module.addChannel(null, mockChannel);
        Collection<Channel<BytesMessage>> channels = module.getAllChannels();

        fail("should have thrown an exception, channel count = " + channels.size());
    }

    @Test(expected = NullArgumentException.class)
    public void addChannelShouldFailWhenMaximumBufferingPeriodIsNull() {
        Integer nullMaxBufferingPeriod = null;
        module.addChannel(12345L, mockChannel, nullMaxBufferingPeriod);
        Collection<Channel<BytesMessage>> channels = module.getAllChannels();

        fail("should have thrown an exception, channel count = " + channels.size());
    }

    @Test(expected = NullArgumentException.class)
    public void addChannelShouldFailWhenChannelIsNull() {
        module.addChannel(700L, null);
        Collection<Channel<BytesMessage>> channels = module.getAllChannels();

        fail("should have thrown an exception, channel count = " + channels.size());
    }

    @Test
    public void addChannelWithValidArguments() {
        module.addChannel(700L, mockChannel);
        Collection<Channel<BytesMessage>> channels = module.getAllChannels();

        assertEquals(1, channels.size());
        assertSame(mockChannel, channels.toArray()[0]);
    }

    @Test
    public void addChannelWithValidChannelAndMaximumBufferingPeriod() {
        final long sessionId = 700L;
        final Integer maxBufferingPeriod = 300;
        module.addChannel(sessionId, mockChannel, maxBufferingPeriod);
        Collection<Channel<BytesMessage>> channels = module.getAllChannels();

        assertEquals(1, channels.size());
        assertSame(mockChannel, channels.toArray()[0]);

        Optional<Integer> bufferingSize = module.getMaximumBufferingPeriodInSecond(sessionId);
        assertTrue(bufferingSize.isPresent());
        assertEquals(maxBufferingPeriod, bufferingSize.get());
    }

    @Test
    public void getChannelWhenSessionNotFound() {
        module.addChannel(700L, mockChannel);

        Optional<Channel<BytesMessage>> channel = module.getChannel(100L);

        assertFalse(channel.isPresent());
    }

    @Test
    public void getChannelWhenSessionFound() {
        long sessionId = 500L;
        module.addChannel(sessionId, mockChannel);

        Optional<Channel<BytesMessage>> channel = module.getChannel(sessionId);

        assertTrue(channel.isPresent());
        assertSame(mockChannel, channel.get());
    }

    @Test
    public void getMaximumBufferingPeriodWhenSessionNotFound() {
        module.addChannel(700L, mockChannel, 500);

        Optional<Integer> bufferSize = module.getMaximumBufferingPeriodInSecond(100L);

        assertFalse(bufferSize.isPresent());
    }

    @Test
    public void getMaximumBufferingPeriodWhenSessionFound() {
        final long sessionId = 500L;
        final Integer maxBufferingPeriod = 3600;
        module.addChannel(sessionId, mockChannel, maxBufferingPeriod);

        Optional<Integer> bufferSize = module.getMaximumBufferingPeriodInSecond(sessionId);

        assertTrue(bufferSize.isPresent());
        assertSame(maxBufferingPeriod, bufferSize.get());
    }

    @Test
    public void getSubscriptionCallbackWhenSessionIdFound() {
        long sessionId = 250L;

        module.addChannel(sessionId, mockChannel, 3600, mockPersistCallback);
        PersistCallback callback = module.getCallback(sessionId);

        assertNotNull(callback);
        assertSame(mockPersistCallback, callback);
    }

    @Test
    public void getSubscriptionCallbackWhenSessionIdNotFound() {
        long sessionId = 250L;

        module.addChannel(sessionId, mockChannel, 3600, mockPersistCallback);
        PersistCallback callback = module.getCallback(25L);

        assertNull(callback);
    }

    @Test
    public void removeChannelWhenSessionNotFound() {
        module.addChannel(100L, mockChannel);

        assertFalse(module.removeChannel(200L));
    }

    @Test
    public void stopChannelSuccess() {
        long sessionId = 250L;

        module.addChannel(sessionId, mockChannel);

        assertTrue(module.closeChannel(sessionId));

        Collection<Channel<BytesMessage>> channels = module.getAllChannels();
        assertEquals(1, channels.size());
    }
    
    @Test
    public void removeChannelSuccess() {
        long sessionId = 250L;

        module.addChannel(sessionId, mockChannel);

        assertTrue(module.removeChannel(sessionId));

        Collection<Channel<BytesMessage>> channels = module.getAllChannels();
        assertEquals(0, channels.size());
    }

    @Test
    public void addSubscriptionCallbackWithValidArguments() {
        long sessionId = 250L;

        module.addChannel(sessionId, mockChannel, 3600, mockPersistCallback);
        Collection<PersistCallback> callbacks = module.getAllCallbacks();

        assertEquals(1, callbacks.size());
        assertSame(mockPersistCallback, callbacks.toArray()[0]);
    }

    @Test(expected = NullArgumentException.class)
    public void addCallbackShouldFailWhenSessionIdIsNull() {
        StorageModule module = new StorageModule();
        module.addChannel(null, mockChannel, 3600, mockPersistCallback);
        Collection<PersistCallback> callbacks = module.getAllCallbacks();

        fail("should have thrown an exception, channel count = " + callbacks.size());
    }

    @Test
    public void removeCallbackWhenSessionNotFound() {
        StorageModule module = new StorageModule();
        module.addChannel(100L, mockChannel, 3600, mockPersistCallback);

        assertFalse(module.removeSubscriptionCallback(200L));
    }

    @Test
    public void removeCallbackSuccess() {
        long sessionId = 250L;
        StorageModule module = new StorageModule();
        module.addChannel(sessionId, mockChannel, 3600, mockPersistCallback);

        assertTrue(module.removeSubscriptionCallback(sessionId));

        Collection<PersistCallback> callbacks = module.getAllCallbacks();
        assertEquals(0, callbacks.size());
    }

    @Test
    public void stopSessionChannelSuccess() throws Exception {
        long sessionId = 250L;

        StorageModule module = new StorageModule();
        module.addChannel(sessionId, mockChannel, new Integer(3600), mockPersistCallback);

        module.stopSessionChannel(sessionId);

        Collection<Channel<BytesMessage>> channels = module.getAllChannels();
        assertEquals(0, channels.size());
    }
    
    @Test
    public void addCorrectNumberOfCommandsToRpcRegistry() throws Exception {
        final long NUMBER_OF_RPC_METHODS = 13;
        module.run(mockNode, mockAttributes);
        assertEquals(NUMBER_OF_RPC_METHODS, module.getRpcServer().registry().size());
    }

    @Test
    public void receiveAndRespondToClientRequestOnCommandChannel() throws Exception {
        Node node = NodeFactory.buildMockNode();
        module.run(node, mockAttributes);

        URI listenChannel = URI.create("ndn:/intel/node/1/module/1/module-CMD/listenChannel$");
        // TODO: STORAGE_COMMAND_CHANNEL: Chaned to match new global command channel. Will need to change it back when the channel changes in the actual code.
        URI storageCommandChannelName = URI.create(BaseMessage.COMMAND_CHANNEL_NAME);

        // Setup client and make command request
        RpcClient rpcClient = Rpc.newClient(node.channels(), storageCommandChannelName);
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("listenChannel", listenChannel.toString());

        CompletableFuture<CommandResponse> future = rpcClient.call(CommandRequest.from("start", inputs));
        CommandResponse response = future.get(REMOTE_CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertFalse(response.err);
    }
}