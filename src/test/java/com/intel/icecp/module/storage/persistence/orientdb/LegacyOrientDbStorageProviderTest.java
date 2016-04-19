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

package com.intel.icecp.module.storage.persistence.orientdb;

import com.intel.icecp.module.query.Id;
import com.intel.icecp.module.query.Queries;
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.InconsistentStateException;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

/**
 * Unit tests for LegacyOrientDbStorageProvider implementation.
 *
 */
public class LegacyOrientDbStorageProviderTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    private OrientVertex mockOrientVertex;
    private LegacyOrientDbStorageProvider storageProvider;
    private LegacyOrientDbStorageProvider storageSpy;
    private OrientGraph graph;

    @Before
    public void setup() throws InconsistentStateException {
        MockitoAnnotations.initMocks(this);

        OrientDbConfiguration configuration = new OrientDbConfiguration();
        configuration.setStorageType(OrientDbStorageType.IN_MEMORY_GRAPH);
        graph = GraphDbUtils.getGraphDbInstance(configuration);

        OrientDbNamespace.setupSchemata(graph);
        storageProvider = new LegacyOrientDbStorageProvider(graph);

        // for using default constructor, need to make sure to turn off the db transaction by default
        storageSpy = Mockito.spy(new LegacyOrientDbStorageProvider());
        storageSpy.graphDbInstance.setAutoStartTx(false);
    }

    @After
    public void after() {
        graph.drop();
    }

    @Test
    public void testGetChannels() throws Exception {
        Set<URI> channelSet;
        channelSet = storageProvider.getChannels();
        assertTrue(channelSet.isEmpty());
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId = storageProvider.createSession(channelName);
        channelSet = storageProvider.getChannels();
        assertTrue(channelSet.size() > 0);
        assertEquals(1, channelSet.size());
        assertEquals(channelName, channelSet.iterator().next());
        storageProvider.deleteSession(sessionId);

    }

    @Test
    public void testCreateSessionId() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId = storageProvider.createSession(channelName);
        System.out.println("Got sessionId = " + sessionId + " for channel name: " + channelName);
        assertTrue(sessionId != 0);
        storageProvider.deleteSession(sessionId);
        // test null name or empty name
        sessionId = storageProvider.createSession(null);
        assertEquals(0, sessionId);
    }

    @Test
    public void createSessionWithDefaultPeriodWhenNotSpecified() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId = storageProvider.createSession(channelName);

        Vertex sessionVertex = storageProvider.getSessionVertexById(sessionId);
        int maxBufferPeriod = sessionVertex.getProperty(OrientDbNamespace.SESSION_MAX_BUFFER_PERIOD_IN_SEC_KEY);
        assertEquals(maxBufferPeriod, StorageModule.DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC);
    }

    @Test
    public void purgeMessagesWithExpiredBufferPeriodsDuringGet() throws Exception {
        int totalNumberMessages = 5;

        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testPurgeExpiredMessages");
        long sessionId = storageProvider.createSession(channelName, 1);

        createTestMessages(sessionId, totalNumberMessages);

        assertEquals(totalNumberMessages, storageProvider.getSessionSize(sessionId));
        // wait for sometime so that the messages will be expired 
        synchronized (this) {
            this.wait(2000);
        }

        // should purge the messages since the max buffer time will have expired.
        List<PersistentMessage> retrievedMessages = storageProvider.getMessages(sessionId);
        assertEquals(0, retrievedMessages.size());
        assertEquals(0, storageProvider.getSessionSize(sessionId));
    }

    @Test
    public void notPurgeMessagesWithoutExpiredBufferPeriodsDuringGet() throws Exception {
        int totalNumberMessages = 5;

        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testPurgeNonExpiredMessages");
        long sessionId = storageProvider.createSession(channelName, 60);

        createTestMessages(sessionId, totalNumberMessages);

        assertEquals(totalNumberMessages, storageProvider.getSessionSize(sessionId));

        // should purge the messages since the max buffer time will have expired.
        List<PersistentMessage> retrievedMessages = storageProvider.getMessages(sessionId);
        assertEquals(totalNumberMessages, retrievedMessages.size());
        assertEquals(totalNumberMessages, storageProvider.getSessionSize(sessionId));
    }

    @Test
    public void setRenamedSessionWithSameMaxBufferPeriodAsPreviousSession() throws Exception {
        int bufferPeriodInSeconds = 60;
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testRenameSameBufferPeriod");
        long sessionId = storageProvider.createSession(channelName, bufferPeriodInSeconds);

        long newSessionId = storageProvider.renameSession(channelName, sessionId);
        Vertex newSessionVertex = storageProvider.getSessionVertexById(newSessionId);

        int maxBufferPeriod = newSessionVertex.getProperty(OrientDbNamespace.SESSION_MAX_BUFFER_PERIOD_IN_SEC_KEY);
        assertEquals(maxBufferPeriod, bufferPeriodInSeconds);
    }

    @Test
    public void testGetSessionIdSetByQueryChannelName() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        Set<Collection<Long>> sessionIdSet = storageProvider.getSessions(channelName);
        assertTrue(sessionIdSet.size() > 0);
        storageProvider.deleteSession(sessionId1);

        channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId2");
        Set<Collection<Long>> sessionIdSet2 = storageProvider.getSessions(channelName);
        assertTrue(sessionIdSet2.size() == 0);
        long sessionId2 = storageProvider.createSession(channelName);
        sessionIdSet2 = storageProvider.getSessions(channelName);
        assertTrue(sessionIdSet2.size() > 0);
        assertEquals(1, sessionIdSet2.size());
        assertEquals(sessionId2, sessionIdSet2.iterator().next().iterator().next().longValue());
        storageProvider.deleteSession(sessionId2);
    }

    @Test
    public void testGetSessionIdSetByQuerySessionId() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        Set<Collection<Long>> sessionIdSet = storageProvider.getSessions(sessionId1);
        assertTrue(sessionIdSet.size() > 0);
        assertEquals(1, sessionIdSet.size());
        assertEquals(sessionId1, sessionIdSet.iterator().next().iterator().next().longValue());
        storageProvider.deleteSession(sessionId1);

        channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId2");
        Set<Collection<Long>> sessionIdSet2 = storageProvider.getSessions(channelName);
        assertTrue(sessionIdSet2.size() == 0);
        long sessionId2 = storageProvider.createSession(channelName);
        sessionIdSet2 = storageProvider.getSessions(sessionId2);
        assertTrue(sessionIdSet2.size() > 0);
        assertEquals(1, sessionIdSet2.size());
        assertEquals(sessionId2, sessionIdSet2.iterator().next().iterator().next().longValue());
        storageProvider.deleteSession(sessionId2);
    }

    @Test
    public void testGetSessionsOnlyWithActiveMessages() throws Exception{
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionIdWithActiveMessages");
        long sessionId1 = storageProvider.createSession(channelName);

        PersistentMessage[] firstSessionMsgs = createTestMessages(sessionId1, 2);

        // rename to a new session
        long renamedSessionId = storageProvider.renameSession(channelName, sessionId1);

        PersistentMessage[] secondSessionMsgs = createTestMessages(renamedSessionId, 3);

        long renamedSessionId2 = storageProvider.renameSession(channelName, renamedSessionId);

        Set<Collection<Long>> sessionIdSet = storageProvider.getSessionsWithActiveMessages(renamedSessionId2);
        assertEquals(3, sessionIdSet.iterator().next().size());

        storageProvider.deleteMessage(sessionId1, firstSessionMsgs[0].getId());
        storageProvider.deleteMessage(sessionId1, firstSessionMsgs[1].getId());

        sessionIdSet = storageProvider.getSessionsWithActiveMessages(renamedSessionId2);
        assertEquals(2, sessionIdSet.iterator().next().size());
        assertFalse(sessionIdSet.iterator().next().contains(sessionId1));

        storageProvider.deleteSession(sessionId1);
        storageProvider.deleteSession(renamedSessionId);
        storageProvider.deleteSession(renamedSessionId2);
    }

    @Test
    public void testGetSessionIdSetWithRenamesByQueryChannelName() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        Set<Collection<Long>> sessionIdSet = storageProvider.getSessions(channelName);
        assertTrue(sessionIdSet.size() > 0);
        // rename to a new session
        long renamedSessionId = storageProvider.renameSession(channelName, sessionId1);
        assertTrue(renamedSessionId != sessionId1);
        // get the linked sessions:
        Set<Collection<Long>> linkedSessionIdSet = storageProvider.getSessions(channelName);
        assertTrue(linkedSessionIdSet.size() > 0);
        assertEquals(1, linkedSessionIdSet.size());
        assertEquals(renamedSessionId, linkedSessionIdSet.iterator().next().iterator().next().longValue());
        storageProvider.deleteSession(sessionId1);
        storageProvider.deleteSession(renamedSessionId);
    }

    @Test
    public void testGetSessionIdSetWithRenamesByQuerySessionId() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        Set<Collection<Long>> sessionIdSet = storageProvider.getSessions(sessionId1);
        assertTrue(sessionIdSet.size() > 0);
        // rename to a new session
        long renamedSessionId = storageProvider.renameSession(channelName, sessionId1);
        assertTrue(renamedSessionId != sessionId1);
        // get the linked sessions:
        Set<Collection<Long>> linkedSessionIdSet = storageProvider.getSessions(renamedSessionId);
        assertTrue(linkedSessionIdSet.size() > 0);
        assertEquals(1, linkedSessionIdSet.size());
        assertEquals(renamedSessionId, linkedSessionIdSet.iterator().next().iterator().next().longValue());
        storageProvider.deleteSession(sessionId1);
        storageProvider.deleteSession(renamedSessionId);
    }

    @Test
    public void testGetSessionIdSetWithRenameAndDeleteByQuerySessionId() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        Set<Collection<Long>> sessionIdSet = storageProvider.getSessions(sessionId1);
        assertTrue(sessionIdSet.size() > 0);
        // rename to a new session
        long renamedSessionId = storageProvider.renameSession(channelName, sessionId1);
        long renamedSessionId2 = storageProvider.renameSession(channelName, renamedSessionId);
        // delete the middle sessionId
        storageProvider.deleteSession(renamedSessionId);

        assertTrue(renamedSessionId2 != sessionId1);
        // get the linked sessions:
        Set<Collection<Long>> linkedSessionIdSet = storageProvider.getSessions(renamedSessionId2);
        assertTrue(linkedSessionIdSet.size() > 0);
        assertEquals(1, linkedSessionIdSet.size());
        assertEquals(renamedSessionId2, linkedSessionIdSet.iterator().next().iterator().next().longValue());
        storageProvider.deleteSession(sessionId1);
        storageProvider.deleteSession(renamedSessionId2);
    }

    @Test
    public void testDeleteSession() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        storageProvider.deleteSession(sessionId1);

        // delete some non-existing sessionId...
        exception.expect(StorageModuleException.class);
        storageProvider.deleteSession(-1);
    }

    @Test
    public void deleteRangeOfMessagesWhenSessionAndMessagesExists() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testDeleteRange");
        long session1 = storageProvider.createSession(channelName);

        PersistentMessage[] messages = createDeleteRangeMessages(session1, 10);

        storageProvider.deleteMessagesByRange(session1, messages[3].getId(), messages[6].getId());
        assertEquals(6, storageProvider.getSessionSize(session1));
    }

    @Test
    public void throwWhenStartSeqNumDoesNotExistInSequence() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testDeleteRange");
        long session1 = storageProvider.createSession(channelName);

        PersistentMessage[] messages = createDeleteRangeMessages(session1, 10);

        exception.expect(StorageModuleException.class);
        storageProvider.deleteMessagesByRange(session1, 11, messages[6].getId());
        assertEquals(10, storageProvider.getSessionSize(session1));
    }

    @Test
    public void throwWhenEndSeqNumDoesNotExistInSequence() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testDeleteRange");
        long session1 = storageProvider.createSession(channelName);

        PersistentMessage[] messages = createDeleteRangeMessages(session1, 10);

        exception.expect(StorageModuleException.class);
        storageProvider.deleteMessagesByRange(session1, messages[3].getId(), 11);
        assertEquals(7, storageProvider.getSessionSize(session1));
    }

    private PersistentMessage[] createDeleteRangeMessages(long sessionId, int numberOfMessages) throws Exception {
        PersistentMessage[] messages = createTestMessages(sessionId, numberOfMessages);
        assertEquals(numberOfMessages, storageProvider.getSessionSize(sessionId));
        return messages;
    }

    private long createSessionWithOneMessage(URI channelName) throws Exception {
        long sessionId = storageProvider.createSession(channelName);
        PersistentMessage message = new PersistentMessage(1, System.currentTimeMillis(),
                "New Persistent Message Content #1".getBytes());
        storageProvider.saveMessage(sessionId, message);

        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId);
        assertEquals(1, msgList.size());
        return sessionId;
    }

    @Test
    public void testSavePersistentMessages() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId = createSessionWithOneMessage(channelName);
        storageProvider.deleteSession(sessionId);

    }

    @Test
    public void testSavePersistentMessageWithNonExistingInputs() throws Exception {
        exception.expect(StorageModuleException.class);
        storageProvider.saveMessage(-1, null);

        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        exception.expect(StorageModuleException.class);
        storageProvider.saveMessage(sessionId1, null);
    }

    @Test
    public void testGetMessages() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        PersistentMessage msg1 = new PersistentMessage(1, System.currentTimeMillis(),
                "New Persistent Message Content #1".getBytes());
        storageProvider.saveMessage(sessionId1, msg1);
        PersistentMessage msg2 = new PersistentMessage(2, System.currentTimeMillis(),
                "New Persistent Message Content #2".getBytes());
        storageProvider.saveMessage(sessionId1, msg2);

        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId1);
        assertNotNull(msgList);
        assertTrue(msgList.size() > 0);
        assertEquals(2, msgList.size());
        assertEquals(msg1.getId(), msgList.get(0).getId());
        assertEquals(msg2.getId(), msgList.get(1).getId());
        storageProvider.deleteSession(sessionId1);
    }

    @Test
    public void testGetMessagesWithNonExistingSessionId() throws Exception {
        exception.expect(StorageModuleException.class);
        storageProvider.getMessages(-1);
    }

    @Test
    public void testGetMessagesWithLimitAndOffset() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 5;
        // save several persistentMessages
        PersistentMessage[] msgs = createTestMessages(sessionId1, totalMsgNum);

        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId1, 3, 2);
        assertNotNull(msgList);
        assertTrue(msgList.size() > 0);
        assertEquals(3, msgList.size());
        assertEquals(msgs[2].getId(), msgList.get(0).getId());
        assertEquals(msgs[3].getId(), msgList.get(1).getId());
        assertEquals(msgs[4].getId(), msgList.get(2).getId());

        // now try to get messages beyond max Offset:
        List<PersistentMessage> msgList2 = storageProvider.getMessages(sessionId1, 3, msgs.length);
        assertTrue(msgList2.isEmpty());

        // try to get message beyond max limit numbers
        // it still should returns number of messages up to max limit
        List<PersistentMessage> msgList3 = storageProvider.getMessages(sessionId1, msgs.length + 1, 2);
        assertTrue(msgList3.size() > 0);
        assertEquals(msgs.length - 2, msgList3.size());
        assertEquals(msgs[2].getId(), msgList3.get(0).getId());
        assertEquals(msgs[3].getId(), msgList3.get(1).getId());
        assertEquals(msgs[4].getId(), msgList3.get(2).getId());

        storageProvider.deleteSession(sessionId1);
    }

    @Test
    public void whenLimitZeroGetReturnsNothing() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 5;
        // save several persistentMessages
        createTestMessages(sessionId1, totalMsgNum);

        // limit 0
        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId1, 0, 2);
        assertNotNull(msgList);
        assertTrue(msgList.isEmpty());
    }

    @Test
    public void whenOffsetNegativeGetReturnsNothing() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 5;

        createTestMessages(sessionId1, totalMsgNum);

        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId1, -2, 5);
        assertNotNull(msgList);
        assertTrue(msgList.isEmpty());
    }

    @Test
    public void whenGetSkipsOldestMessagesAreNotReturned() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 5;

        createTestMessages(sessionId1, totalMsgNum);

        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId1, 100, 2);

        assertNotNull(msgList);
        assertEquals(3, msgList.size());
        // make sure the messages are 3, 4, and 5 (the oldest ones 1 and 2 were skipped)
        assertEquals(3, msgList.get(0).getId());
        assertEquals(4, msgList.get(1).getId());
        assertEquals(5, msgList.get(2).getId());
    }

    @Test
    public void testGetMessagesWithNegativeLimitAndOffset() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 5;
        // save several persistentMessages
        createTestMessages(sessionId1, totalMsgNum);

        // invalid sessionId
        long invalidSessionId = -1;
        if (invalidSessionId == sessionId1) {
            invalidSessionId = -2;
        }
        List<PersistentMessage> msgList = storageProvider.getMessages(invalidSessionId, 0, 1);
        assertNotNull(msgList);
        assertTrue(msgList.isEmpty());

        storageProvider.deleteSession(sessionId1);
    }

    /**
     * create test messages with the given timestamp.
     *
     * @param sessionId - sessionId in which to create the messages
     * @param totalMsgNum - number of messages to create on the session.
     * @return messages
     */
    private PersistentMessage[] createTestMessages(long sessionId, int totalMsgNum) throws Exception {
        PersistentMessage[] messages = new PersistentMessage[totalMsgNum];
        for (int i = 0; i < totalMsgNum; i++) {
            int seq = i + 1;
            PersistentMessage msg = new PersistentMessage(seq, System.currentTimeMillis() + seq,
                    ("New Persistent Message Content #" + seq).getBytes());
            messages[i] = msg;
            storageProvider.saveMessage(sessionId, msg);
        }
        return messages;
    }

    @Test
    public void testDeleteMessages() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 5;
        // save several persistentMessages
        createTestMessages(sessionId1, totalMsgNum);

        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId1);
        assertNotNull(msgList);
        assertTrue(msgList.size() > 0);
        assertEquals(totalMsgNum, msgList.size());
        // using non-existing sessionId:
        assertFalse(storageProvider.deleteMessages(sessionId1 == -1 ? -2 : -1));

        // now delete messages:
        boolean ok = storageProvider.deleteMessages(sessionId1);
        assertTrue(ok);
        List<PersistentMessage> msgListAfterDeletion = storageProvider.getMessages(sessionId1);
        assertTrue(msgListAfterDeletion.isEmpty());

        storageProvider.deleteSession(sessionId1);
    }

    @Test
    public void testDeleteMessageWithMessageId() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 5;
        // save several persistentMessages
        PersistentMessage[] messages = createTestMessages(sessionId1, totalMsgNum);

        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId1);
        assertNotNull(msgList);
        assertTrue(msgList.size() > 0);
        assertEquals(totalMsgNum, msgList.size());
        // now delete message:
        PersistentMessage messageToDeleted = messages[2];
        storageProvider.deleteMessage(sessionId1, messageToDeleted.getId());

        List<PersistentMessage> msgListAfterDeletion = storageProvider.getMessages(sessionId1);
        assertTrue(msgListAfterDeletion.size() > 0);
        assertEquals(totalMsgNum - 1, msgListAfterDeletion.size());
        // the original message with index 2 should be gone
        // the message with index 3 now become index 2 and so on
        assertEquals(messages[3].getId(),
                msgListAfterDeletion.get(2).getId());
        assertEquals(messages[4].getId(),
                msgListAfterDeletion.get(3).getId());

        // now try to delete message with non-existing message id:
        storageProvider.deleteMessage(sessionId1, 1234);
        List<PersistentMessage> msgListAfterDeletion2 = storageProvider.getMessages(sessionId1);
        assertTrue(msgListAfterDeletion2.size() > 0);
        assertEquals(totalMsgNum - 1, msgListAfterDeletion2.size());

        storageProvider.deleteSession(sessionId1);
    }

    @Test
    public void testDeleteMessageWithMessageIdUsingInvalidInputs() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testSessionId");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 5;
        // save several persistentMessages
        PersistentMessage[] messages = createTestMessages(sessionId1, totalMsgNum);

        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId1);
        assertNotNull(msgList);
        assertTrue(msgList.size() > 0);
        assertEquals(totalMsgNum, msgList.size());
        // now delete message:
        PersistentMessage messageToDeleted = messages[2];
        exception.expect(StorageModuleException.class);
        storageProvider.deleteMessage(sessionId1 == -1 ? -2 : -1,
                messageToDeleted.getId());

        exception.expect(StorageModuleException.class);
        storageProvider.deleteMessage(sessionId1, -1);

        storageProvider.deleteSession(sessionId1);
    }

    @Test
    public void testGetMessagesWithLimitAndOffsetWhileDeletingMessages() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testDelete");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 20;
        // save several persistentMessages
        PersistentMessage[] msgs = createTestMessages(sessionId1, totalMsgNum);

        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId1, 3, 2);
        assertNotNull(msgList);
        assertTrue(msgList.size() > 0);
        assertEquals(3, msgList.size());
        assertEquals(msgs[2].getId(), msgList.get(0).getId());
        assertEquals(msgs[3].getId(), msgList.get(1).getId());
        assertEquals(msgs[4].getId(), msgList.get(2).getId());

        // now deleting messages with index 2 - 4
        storageProvider.deleteMessage(sessionId1, msgs[2].getId());
        storageProvider.deleteMessage(sessionId1, msgs[3].getId());
        storageProvider.deleteMessage(sessionId1, msgs[4].getId());

        List<PersistentMessage> msgList2 = storageProvider.getMessages(sessionId1, 3, 2);
        assertNotNull(msgList2);
        assertEquals(3, msgList2.size());
        assertEquals(msgs[5].getId(), msgList2.get(0).getId());
        assertEquals(msgs[6].getId(), msgList2.get(1).getId());
        assertEquals(msgs[7].getId(), msgList2.get(2).getId());

        storageProvider.deleteSession(sessionId1);
    }

    @Test
    public void testGetMessagesForMultipleSessionsWithLimitAndOffsetWhileDeletingMessages() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testDeleteMultipleSession");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 20;
        // save several persistentMessages
        PersistentMessage[] msgs = createTestMessages(sessionId1, totalMsgNum);

        List<PersistentMessage> msgList = storageProvider.getMessages(sessionId1, 3, 2);
        assertEquals(3, msgList.size());
        assertEquals(msgs[2].getId(), msgList.get(0).getId());
        assertEquals(msgs[3].getId(), msgList.get(1).getId());
        assertEquals(msgs[4].getId(), msgList.get(2).getId());

        long sessionId2 = storageProvider.createSession(channelName);
        // make sessionId2 also holds msg with index 2, 3, 4
        PersistentMessage[] msgs2 = createTestMessages(sessionId2, 3);

        List<PersistentMessage> msgList2 = storageProvider.getMessages(sessionId2);
        assertEquals(3, msgList.size());
        assertEquals(msgs2[0].getId(), msgList2.get(0).getId());
        assertEquals(msgs2[1].getId(), msgList2.get(1).getId());
        assertEquals(msgs2[2].getId(), msgList2.get(2).getId());

        // now deleting messages with index 2 - 4
        storageProvider.deleteMessage(sessionId1, msgs[2].getId());
        storageProvider.deleteMessage(sessionId1, msgs[3].getId());
        storageProvider.deleteMessage(sessionId1, msgs[4].getId());

        List<PersistentMessage> msgList3 = storageProvider.getMessages(sessionId1, 3, 2);
        assertNotNull(msgList3);
        assertEquals(3, msgList3.size());
        assertEquals(msgs[5].getId(), msgList3.get(0).getId());
        assertEquals(msgs[6].getId(), msgList3.get(1).getId());
        assertEquals(msgs[7].getId(), msgList3.get(2).getId());

        List<PersistentMessage> msgList4 = storageProvider.getMessages(sessionId2, 3, 1);
        assertEquals(2, msgList4.size());
        assertEquals(msgs2[1].getId(), msgList4.get(0).getId());
        assertEquals(msgs2[2].getId(), msgList4.get(1).getId());

        storageProvider.deleteSession(sessionId1);
        storageProvider.deleteSession(sessionId2);
    }

    @Test
    public void testGetSessionSize() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testGetSize");
        long sessionId1 = storageProvider.createSession(channelName);
        final int totalMsgNum = 20;
        // save several persistentMessages
        PersistentMessage[] msgs = createTestMessages(sessionId1, totalMsgNum);

        int session1Size = storageProvider.getSessionSize(sessionId1);
        assertEquals(totalMsgNum, session1Size);

        long sessionId2 = storageProvider.createSession(channelName);
        createTestMessages(sessionId2, 3);

        int session2Size = storageProvider.getSessionSize(sessionId2);
        assertEquals(3, session2Size);

        // now deleting messages with index 2 - 4
        storageProvider.deleteMessage(sessionId1, msgs[2].getId());
        storageProvider.deleteMessage(sessionId1, msgs[3].getId());
        storageProvider.deleteMessage(sessionId1, msgs[4].getId());

        int session1SizeAfterDeletion = storageProvider.getSessionSize(sessionId1);
        assertEquals(totalMsgNum - 3, session1SizeAfterDeletion);

        int session2SizeAfterDeletion = storageProvider.getSessionSize(sessionId2);
        assertEquals(3, session2SizeAfterDeletion);

        storageProvider.deleteSession(sessionId1);
        storageProvider.deleteSession(sessionId2);

        // non-existing sessionId test
        assertEquals(0, storageProvider.getSessionSize(-1));
    }

    @Test
    public void throwWhenOldSessionVertexNotFound() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testNoOldVertex");
        long missingSessionId = 111L;

        exception.expect(StorageModuleException.class);
        storageProvider.renameSession(channelName, missingSessionId);
    }

    @Test
    public void throwWhenNewSessionVertexNotFound() throws Exception {
        URI channelName = new URI("ndn://icecp-storage-module.intel.com/testNoNewVertex");

        long sessionId = storageSpy.createSession(channelName);
        doReturn(null).when(storageSpy).getSessionVertexById(not(eq(sessionId)));

        exception.expect(StorageModuleException.class);
        storageSpy.renameSession(channelName, sessionId);
    }

    @Test
    public void throwWhenVertexNotFoundWhenCreatingNewEdge() throws Exception {
        String channelName = "ndn://icecp-storage-module.intel.com/testNoNewEdge";
        long sessionId = createNewSession(channelName);

        doReturn(mockOrientVertex).when(storageSpy).getSessionVertexById(eq(sessionId));

        exception.expect(StorageModuleException.class);
        storageSpy.renameSession(new URI(channelName), sessionId);
    }

    @Test
    public void renameSessionHasCorrectEdgeAndVertexCount() throws Exception {
        String channelName = "ndn://icecp-storage-module.intel.com/renameTest";
        long sessionId = createNewSession(channelName);
        storageProvider.renameSession(new URI(channelName), sessionId);

        assertEquals(1, storageProvider.graphDbInstance.countEdges());
        assertEquals(2, storageProvider.graphDbInstance.countVertices());
    }


    @Test
    public void renameSessionSetsEdgeCorrectly() throws Exception {
        String channelName = "ndn://icecp-storage-module.intel.com/renameTest";
        long sessionId = createNewSession(channelName);
        long newSessionId = storageProvider.renameSession(new URI(channelName), sessionId);

        Vertex activeVertex = storageProvider.getSessionVertexById(newSessionId);
        long value = activeVertex.getProperty(OrientDbNamespace.SESSION_ID_KEY);
        assertEquals(newSessionId, value); // Verify property was set correctly on the vertex

        // Verify active vertex has the correct number of edges.
        int activeVertexEdgeCount = 0;

        for (Edge edge : activeVertex.getEdges(Direction.BOTH)) {
            Vertex vertex = edge.getVertex(Direction.IN);
            long oldSessionId = vertex.getProperty(OrientDbNamespace.SESSION_ID_KEY);

            // Verify the activeVertex is pointing to the oldVertex.
            assertEquals(oldSessionId, sessionId);
            activeVertexEdgeCount++;
        }

        assertEquals(activeVertexEdgeCount, 1);
    }

    @Test
    public void testDeleteActiveSessionWithLinkedSessionEdge() throws Exception {
        String channelName = "ndn://icecp-storage-module.intel.com/deleteActiveLinkedSession";
        long sessionId = createNewSession(channelName);

        long newSessionId = storageProvider.renameSession(new URI(channelName), sessionId);
        storageProvider.deleteSession(newSessionId);

        // Verify that deleted vertex no longer exists.
        assertNull(storageProvider.getSessionVertexById(newSessionId));

        Vertex activeVertex = storageProvider.getSessionVertexById(sessionId);

        // Verify active vertex has the correct number of edges.
        int activeVertexEdgeCount = 0;

        for (Edge ignored : activeVertex.getEdges(Direction.BOTH)) {
            activeVertexEdgeCount++;
        }

        assertEquals(activeVertexEdgeCount, 0);
    }

    @Test
    public void testDeleteInactiveSessionWithLinkedSessionEdge() throws Exception {
        String channelName = "ndn://icecp-storage-module.intel.com/deleteActiveLinkedSession";
        long sessionId = createNewSession(channelName);

        long newSessionId = storageProvider.renameSession(new URI(channelName), sessionId);
        storageProvider.deleteSession(sessionId);

        // Verify that deleted vertex no longer exists.
        assertNull(storageProvider.getSessionVertexById(sessionId));

        Vertex activeVertex = storageProvider.getSessionVertexById(newSessionId);

        // Verify active vertex has the correct number of edges.
        assertEquals(getVertexEdgeCount(activeVertex), 0);
    }

    @Test
    public void testDeleteInactiveSessionWithOneMessageAndLinkedSessionEdge() throws Exception {
        String channelName = "ndn://icecp-storage-module.intel.com/deleteActiveLinkedSession";
        long sessionId = createSessionWithOneMessage(new URI(channelName));

        long newSessionId = storageProvider.renameSession(new URI(channelName), sessionId);
        Vertex oldVertex = storageProvider.getSessionVertexById(sessionId);

        // Verify that the previous session has 2 edges (to message and to new message)
        assertEquals(getVertexEdgeCount(oldVertex), 2);

        storageProvider.deleteSession(sessionId);

        // Verify that deleted vertex no longer exists.
        assertNull(storageProvider.getSessionVertexById(sessionId));

        Vertex activeVertex = storageProvider.getSessionVertexById(newSessionId);

        assertEquals(getVertexEdgeCount(activeVertex), 0);
    }

    @Test
    public void testDeleteMiddleSessionWithThreeLinkedSessionEdge() throws Exception {
        String channelName = "ndn://icecp-storage-module.intel.com/deleteActiveLinkedSession";
        long sessionId = createNewSession(channelName);

        long secondSessionId = storageProvider.renameSession(new URI(channelName), sessionId);
        long thirdSessionId = storageProvider.renameSession(new URI(channelName), sessionId);

        storageProvider.deleteSession(secondSessionId);

        // Verify that deleted vertex no longer exists.
        assertNull(storageProvider.getSessionVertexById(secondSessionId));

        Vertex activeVertex = storageProvider.getSessionVertexById(thirdSessionId);

        for (Edge edge : activeVertex.getEdges(Direction.BOTH)) {
            Vertex vertex = edge.getVertex(Direction.IN);
            long firstSessionId = vertex.getProperty(OrientDbNamespace.SESSION_ID_KEY);

            // Verify the activeVertex is pointing to the firstVertex and not the second vertex.
            assertEquals(firstSessionId, sessionId);
        }
    }

    private int getVertexEdgeCount(Vertex vertex) {
        // Verify active vertex has the correct number of edges.
        int activeVertexEdgeCount = 0;

        for (Edge ignored : vertex.getEdges(Direction.BOTH)) {
            activeVertexEdgeCount++;
        }

        return activeVertexEdgeCount;
    }

    @Test
    public void throwExceptionWhenSessionVertexNotFound() throws Exception {
        Long SESSION_ID = 123456789L;
        doThrow(StorageModuleException.class).when(storageSpy).doesSessionIdExist(eq(SESSION_ID));
        doReturn(null).when(storageSpy).getSessionVertexById(eq(SESSION_ID));

        exception.expect(StorageModuleException.class);
        storageSpy.deleteSession(SESSION_ID);
    }

    @Test
    public void renameAndDeleteSessionWithCurrentEdgeAndVertexCount() throws Exception {
        String channelName = "ndn://icecp-storage-module.intel.com/renameAndDeleteTest";
        long sessionId = createNewSession(channelName);

        long newSessionId_1 = storageProvider.renameSession(new URI(channelName), sessionId);
        storageProvider.renameSession(new URI(channelName), newSessionId_1);
        storageProvider.deleteSession(newSessionId_1);

        long edgeCount = storageProvider.graphDbInstance.countEdges();
        long vertexCount = storageProvider.graphDbInstance.countVertices();

        assertEquals(1, edgeCount);
        assertEquals(2, vertexCount);
    }

    @Test
    public void retrieveOnlyActiveMessages() throws Exception {
        URI channelName = new URI("mock:/storage/retrieve/active");
        long sessionId1 = storageProvider.createSession(channelName);
        PersistentMessage msg1 = new PersistentMessage(1, 1, "#1".getBytes());
        storageProvider.saveMessage(sessionId1, msg1);
        PersistentMessage msg2 = new PersistentMessage(2, 2, "#2".getBytes());
        storageProvider.saveMessage(sessionId1, msg2);

        TaggedOrientDbStorageProvider tsp = new TaggedOrientDbStorageProvider(storageProvider.graphDbInstance);
        Set<Id> tagged = tsp.tag(Queries.fromId(1), new Tag(OrientDbNamespace.INACTIVE_TAG));
        assertEquals(1, tagged.size());

        List<PersistentMessage> messages = storageProvider.getMessages(sessionId1);
        assertEquals(1, messages.size());
        assertEquals(2, messages.get(0).getId());
    }

    private long createNewSession(String channelName) throws Exception {
        long sessionId = storageProvider.createSession(new URI(channelName));
        System.out.println("SessionId = " + sessionId + " for channel name: " + channelName);
        return sessionId;
    }
}