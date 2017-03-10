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
import com.intel.icecp.core.Message;
import com.intel.icecp.core.Node;
import com.intel.icecp.core.attributes.Attributes;
import com.intel.icecp.core.attributes.IdAttribute;
import com.intel.icecp.core.management.Channels;
import com.intel.icecp.core.metadata.Persistence;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class MessagesITTest {
    private static long MODULE_ID = 12;
    private final Long SESSION_ID = 123456789L;
    private final Long NEW_SESSION_ID = 9876543210L;
    private URI mockUri;
    private StorageModule module;
    @Mock
    private Node mockNode;
    @Mock
    private Channels mockChannels;
    @Mock
    private Channel<Message> mockResponseChannel;
    @Mock
    private StorageProvider mockProvider;
    @Mock
    private Channel<Message> mockCmdChannel;
    @Mock
    private Attributes mockAttributes;

    @SuppressWarnings("unchecked")
    @Before
    public void before() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(mockNode.channels()).thenReturn(mockChannels);
        when(mockNode.channels().openChannel(any(URI.class), (Class<Message>) any(), any(Persistence.class))).thenReturn(mockResponseChannel);

        mockUri = new URI("test://testuri");
        module = new StorageModule(mockProvider);
    }

    private void setMockMethods() throws Exception {
        when(mockAttributes.get(eq(IdAttribute.class))).thenReturn(MODULE_ID);
        when(mockNode.getDefaultUri()).thenReturn(mockUri);
        when(mockNode.openChannel(any(URI.class), any(), any())).thenReturn(mockCmdChannel);

        when(mockProvider.createSession(any(), anyInt())).thenReturn(SESSION_ID);
    }

    @Test
    public void testRenameUpdatesChannelsMapSuccess() throws Exception {
        startMockModule();

        when(mockProvider.renameSession(mockCmdChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);

        Long sessionId = createStartMessage();
        assertNotNull(sessionId);

        RenameMessage rename = new RenameMessage(sessionId);
        Object resp = rename.onCommandMessage(module);

        Long newSessionId = (Long) resp;
        assertNotNull(newSessionId);

        assertEquals(Optional.empty(), module.getChannel(sessionId));
        assertNotNull(module.getChannel(newSessionId).get());

        // Verify listening channel got updated with renamed sessionId
        assertTrue(module.getChannel(newSessionId).isPresent());
        assertEquals(mockCmdChannel, module.getChannel(newSessionId).get());
    }

    @Test
    public void testRenameUpdatesCallbacksMapSuccess() throws Exception {
        startMockModule();

        when(mockProvider.renameSession(mockCmdChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);

        Long sessionId = createStartMessage();
        assertNotNull(sessionId);

        RenameMessage rename = new RenameMessage(sessionId);
        Object resp = rename.onCommandMessage(module);
        Long newSessionId = (Long) resp;

        assertNotNull(newSessionId);

        assertNull(module.getCallback(sessionId));
        assertNotNull(module.getCallback(newSessionId));

        // Verify callback got updated with renamed sessionId
        assertEquals(newSessionId, module.getCallback(newSessionId).getSessionId());
    }

    @Test
    public void testDeleteSessionWithNoLinksUpdatesChannelsMapSuccess() throws Exception {
        startMockModule();

        Long sessionId = createStartMessage();
        assertNotNull(sessionId);

        createDeleteMessage(sessionId);
        assertEquals(Optional.empty(), module.getChannel(sessionId));
    }

    private void startMockModule() throws Exception {
        setMockMethods();
        module.run(mockNode, mockAttributes);
    }

    @Test
    public void testDeleteSessionWithNoLinksUpdatesCallbacksMapSuccess() throws Exception {
        startMockModule();

        Long sessionId = createStartMessage();
        assertNotNull(sessionId);

        createDeleteMessage(sessionId);
        assertNull(module.getCallback(sessionId));
    }

    @Test
    public void testDeleteActiveSessionWithLinksUpdatesChannelsMapSuccess() throws Exception {
        startMockModule();

        when(mockProvider.renameSession(mockCmdChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);
        when(mockProvider.getPreviousSession(NEW_SESSION_ID)).thenReturn(SESSION_ID);
        mockProvider.deleteSession(NEW_SESSION_ID);

        Long sessionId = createStartMessage();
        assertNotNull(sessionId);

        Long newSessionId = createRenameMessage(sessionId);
        assertNotNull(newSessionId);

        createDeleteMessage(newSessionId);

        assertTrue(module.getChannel(sessionId).isPresent());
        assertEquals(Optional.empty(), module.getChannel(newSessionId));
        assertEquals(mockCmdChannel, module.getChannel(sessionId).get());
    }

    @Test
    public void testDeleteActiveSessionWithLinksUpdatesCallbacksMapSuccess() throws Exception {
        startMockModule();

        when(mockProvider.renameSession(mockCmdChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);
        when(mockProvider.getPreviousSession(NEW_SESSION_ID)).thenReturn(SESSION_ID);

        Long sessionId = createStartMessage();
        assertNotNull(sessionId);

        Long newSessionId = createRenameMessage(sessionId);
        assertNotNull(newSessionId);

        createDeleteMessage(newSessionId);

        assertNull(module.getCallback(newSessionId));
        assertNotNull(module.getCallback(sessionId));

        // Verify callback got updated with renamed sessionId
        assertEquals(sessionId, module.getCallback(sessionId).getSessionId());
    }

    @Test
    public void testDeleteInactiveSessionUpdatesChannelsMapSuccess() throws Exception {
        startMockModule();

        when(mockProvider.renameSession(mockCmdChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);
        when(mockProvider.getPreviousSession(SESSION_ID)).thenReturn(0L);


        Long sessionId = createStartMessage();
        assertNotNull(sessionId);

        Long newSessionId = createRenameMessage(sessionId);
        assertNotNull(newSessionId);

        createDeleteMessage(sessionId);
        assertEquals(Optional.empty(), module.getChannel(sessionId));
    }

    @Test
    public void testDeleteInactiveSessionUpdatesCallbacksMapSuccess() throws Exception {
        startMockModule();

        when(mockProvider.renameSession(mockCmdChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);
        when(mockProvider.getPreviousSession(NEW_SESSION_ID)).thenReturn(SESSION_ID);

        Long sessionId = createStartMessage();
        assertNotNull(sessionId);

        Long newSessionId = createRenameMessage(sessionId);
        assertNotNull(newSessionId);

        createDeleteMessage(sessionId);
        assertNull(module.getCallback(sessionId));
    }

    @Test
    public void testDeleteInactiveSessionWithLinksUpdatesChannelsMapSuccess() throws Exception {
        Long anotherSessionId = 99L;

        startMockModule();

        when(mockProvider.renameSession(mockCmdChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);
        when(mockProvider.renameSession(mockCmdChannel.getName(), NEW_SESSION_ID)).thenReturn(anotherSessionId);
        when(mockProvider.getPreviousSession(NEW_SESSION_ID)).thenReturn(SESSION_ID);


        Long sessionId = createStartMessage();
        assertNotNull(sessionId);

        Long newSessionId = createRenameMessage(sessionId);
        assertNotNull(newSessionId);

        Long anotherRenamedSessionId = createRenameMessage(newSessionId);
        assertNotNull(anotherRenamedSessionId);

        createDeleteMessage(newSessionId);
        assertEquals(Optional.empty(), module.getChannel(newSessionId));
        Mockito.verify(mockProvider, times(1)).getPreviousSession(NEW_SESSION_ID);
    }

    @Test
    public void testDeleteInactiveSessionWithLinksUpdatesCallbacksMapSuccess() throws Exception {
        Long anotherSessionId = 99L;

        startMockModule();

        when(mockProvider.renameSession(mockCmdChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);
        when(mockProvider.renameSession(mockCmdChannel.getName(), NEW_SESSION_ID)).thenReturn(anotherSessionId);

        when(mockProvider.getPreviousSession(NEW_SESSION_ID)).thenReturn(SESSION_ID);

        Long sessionId = createStartMessage();
        assertNotNull(sessionId);

        Long newSessionId = createRenameMessage(sessionId);
        assertNotNull(newSessionId);

        Long anotherRenamedSessionId = createRenameMessage(newSessionId);
        assertNotNull(anotherRenamedSessionId);

        createDeleteMessage(newSessionId);
        assertNull(module.getCallback(newSessionId));
        Mockito.verify(mockProvider, times(1)).getPreviousSession(NEW_SESSION_ID);
    }

    @Test
    public void testGetMessageWithExpiredMessagesSuccess() throws Exception {
        startMockModule();

        // create a short buffer size so that message will be expired soon
        final int maxBufferSizeInSec = 1;
        Long sessionId = createStartMessageWithMaximumBufferingPeriod(maxBufferSizeInSec);
        assertNotNull(sessionId);

        final long currentTick = System.currentTimeMillis();
        final int limit = 10;
        final int offset = 0;
        when(mockProvider.getMessages(sessionId, limit, offset)).thenAnswer(getMessageBasedOnTimeTick(currentTick, maxBufferSizeInSec));

        GetMessage getMessage = new GetMessage(sessionId, limit, offset, "uri://replaychannel");

        Object resp = getMessage.onCommandMessage(module);

        assertNotNull(module.getCallback(sessionId));
        assertEquals(3, resp);
        // now wait for 3 seconds for message to be expired...
        waitFor(3000);

        // now get it again
        getMessage = new GetMessage(sessionId, limit, offset, "uri://replaychannel");
        resp = getMessage.onCommandMessage(module);
        assertNotNull(module.getCallback(sessionId));
        assertEquals(0, resp);
    }

    private void waitFor(final long milliSeconds) throws InterruptedException {
        synchronized (this) {
            this.wait(milliSeconds);
        }
    }

    @Test
    public void testSizeMessageWithExpiredMessagesSuccess() throws Exception {
        startMockModule();

        // create a short buffer size so that message will be expired soon
        final int maxBufferSizeInSec = 1;
        Long sessionId = createStartMessageWithMaximumBufferingPeriod(maxBufferSizeInSec);
        assertNotNull(sessionId);

        when(mockProvider.getSessionSize(sessionId)).thenAnswer(getMessageCountBasedOnTimeTick(System.currentTimeMillis(),
                maxBufferSizeInSec));

        SizeMessage sizeMsg = new SizeMessage(sessionId);

        Object resp = sizeMsg.onCommandMessage(module);

        assertNotNull(module.getCallback(sessionId));
        assertEquals(Integer.valueOf(3), resp);

        // now wait for 3 seconds for message to be expired...
        waitFor(3000);

        // now get it again
        sizeMsg = new SizeMessage(sessionId);
        resp = sizeMsg.onCommandMessage(module);
        assertNotNull(module.getCallback(sessionId));
        assertEquals(Integer.valueOf(0), resp);
    }

    private Answer<List<PersistentMessage>> getMessageBasedOnTimeTick(final long baseTick, final int maxBufferSizeInSec) {
        return invocation -> getMockPersistentMessages(baseTick, maxBufferSizeInSec);
    }

    private Answer<Integer> getMessageCountBasedOnTimeTick(final long baseTick, final int maxBufferSizeInSec) {
        return invocation -> getMockPersistentMessages(baseTick, maxBufferSizeInSec).size();
    }

    private List<PersistentMessage> getMockPersistentMessages(final long baseTick,
                                                              final int maxBufferSizeInSec) {
        List<PersistentMessage> testMsgList = new ArrayList<>();
        if (System.currentTimeMillis() - maxBufferSizeInSec * 1000L <= baseTick) {
            // not expired yet
            testMsgList.add(new PersistentMessage(1, baseTick, new byte[0]));
            testMsgList.add(new PersistentMessage(1, baseTick + 2, new byte[0]));
            testMsgList.add(new PersistentMessage(1, baseTick + 4, new byte[0]));
        }
        return testMsgList;
    }

    private Long createStartMessage() throws StorageModuleException {
        StartMessage start = new StartMessage("uri://listenChannel");
        return (Long) start.onCommandMessage(module);
    }

    private Long createStartMessageWithMaximumBufferingPeriod(final int maxBufPeriodInSec) throws StorageModuleException {
        StartMessage start = new StartMessage("uri://listenChannel", maxBufPeriodInSec);
        return (Long) start.onCommandMessage(module);
    }

    private Long createRenameMessage(Long sessionId) throws StorageModuleException {
        RenameMessage rename = new RenameMessage(sessionId);
        Object renameResp = rename.onCommandMessage(module);
        return ((Long) renameResp);
    }

    private Object createDeleteMessage(Long newSessionId) throws StorageModuleException {
        DeleteSession delete = new DeleteSession(newSessionId);
        return delete.onCommandMessage(module);
    }
}
