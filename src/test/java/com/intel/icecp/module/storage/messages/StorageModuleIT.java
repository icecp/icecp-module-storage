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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.intel.icecp.core.Module;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;

import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import com.intel.icecp.core.Channel;
import com.intel.icecp.core.Node;
import com.intel.icecp.core.attributes.AttributeRegistrationException;
import com.intel.icecp.core.attributes.Attributes;
import com.intel.icecp.core.attributes.IdAttribute;
import com.intel.icecp.core.attributes.ModuleStateAttribute;
import com.intel.icecp.core.management.Channels;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.core.metadata.Persistence;
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.attributes.AckChannelAttribute;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.intel.icecp.module.storage.persistence.orientdb.StorageProviderFacade;
import com.intel.icecp.node.AttributesFactory;
import com.intel.icecp.node.NodeFactory;
import org.junit.rules.ExpectedException;



/**
 * Integration Tests for Storage Module.
 * 
 */

public class StorageModuleIT {

    private static final String STORAGE_COMMAND_URI_STRING = "ndn:/intel/storage/command";
    private static final URI STORAGE_COMMAND_URI = URI.create(STORAGE_COMMAND_URI_STRING);
    private static Node node;
    private static StorageModule storageModule;
    private static StorageProvider storageProvider;
    private CommandAdapter commandAdapter;
    private String listenChannelUriString;
    private Channel<BytesMessage> listenChannel;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void beginStorageModule() throws Exception {
        node = NodeFactory.buildMockNode();

        startStorageModule();

        SecureRandom random = new SecureRandom();
        listenChannelUriString = "ndn:/intel/storage/listen/storagetest" + random.nextInt();
        listenChannel = node.openChannel(URI.create(listenChannelUriString), BytesMessage.class, Persistence.DEFAULT);
    }

    @After
    public void cleanStorage() throws Exception {
        ((StorageProviderFacade)storageModule.getStorageProvider()).drop();
    }

    private Long startSessionSendMessages(int numberMessagesToSend) throws Exception {
        Long sessionId = startSession(listenChannelUriString);

        sendMessagesToStorage(numberMessagesToSend);
        assertEquals(numberMessagesToSend, getSizeThroughCommandAdapter(sessionId));
        assertEquals(numberMessagesToSend, getMessagesThroughCommandAdapter(sessionId, 100, 0));
        return sessionId;
    }

    @Test
    public void receiveCorrectNumberMessagesOnNewSessionAfterRename() throws Exception {
        Long sessionId = startSessionSendMessages(5);

        Long newSessionId = renameSessionThroughCommandAdapter(sessionId);

        assertEquals(0, getSizeThroughCommandAdapter(newSessionId));
        assertEquals(0, storageProvider.getMessages(newSessionId).size());

        startSessionSendMessages(8);
        assertEquals(5, getSizeThroughCommandAdapter(sessionId));
        assertEquals(5, getMessagesThroughCommandAdapter(sessionId, 100, 0));
        assertEquals(8, getSizeThroughCommandAdapter(newSessionId));
        assertEquals(8, getMessagesThroughCommandAdapter(newSessionId, 100, 0));
    }

    @Test
    public void receiveCorrectNumberMessagesAfterDeleteSession() throws Exception {
        Long sessionId = startSessionSendMessages(5);

        Map<String, Object> inputs = new LinkedHashMap<>();
        inputs.put("sessionId", sessionId);
        commandAdapter.deleteSession(inputs);
        assertEquals(0, getSizeThroughCommandAdapter(sessionId));
        Long sessionId2 = startSessionSendMessages(8);
        assertEquals(8, getSizeThroughCommandAdapter(sessionId2));
    }

    @Test
    public void throwWhenInvalidSessionInDeleteSession() throws Exception {
        startSessionSendMessages(5);

        Map<String, Object> inputs = new LinkedHashMap<>();
        inputs.put("sessionId", 123L);
        exception.expect(StorageModuleException.class);
        assertEquals(0, commandAdapter.deleteSession(inputs));
    }

    @Test
    public void handleMultipleSessions() throws Exception {
        Long sessionId = startSessionSendMessages(5);
        Long sessionId2 = startSessionSendMessages(8);
        assertEquals(13, getSizeThroughCommandAdapter(sessionId));
        assertEquals(8, getSizeThroughCommandAdapter(sessionId2));
    }

    @Ignore
    @Test
    public void notReceiveMessagesOnStoppedSession() throws Exception {
        Long sessionId = startSessionSendMessages(5);

        Map<String, Object> inputs = new LinkedHashMap<>();
        inputs.put("sessionId", sessionId);
        commandAdapter.stop(inputs);

        sendMessagesToStorage(3);
        assertEquals(5, getSizeThroughCommandAdapter(sessionId));
        assertEquals(5, getMessagesThroughCommandAdapter(sessionId, 100, 0));
    }

    // because mock channels don't close properly we cannot test this programmatically at this point in time
    @Ignore
    @Test
    public void tagMessagesOnStopRestart() throws Exception {
        // Test the behaviors in the defect EAPE-1793
        // send first batch of messages in which tagging will happen
        Long sessionId = startSessionSendMessages(5);

        Map<String, Object> inputs = new LinkedHashMap<>();
        inputs.put("sessionId", sessionId);
        // stop the storage module
        storageModule.stop(Module.StopReason.USER_DIRECTED);

        // restart storage module
        startStorageModule();

        // sent another batch of messages to cause tagging
        Long restartedSessionId = startSessionSendMessages(5);

        assertEquals(5, getSizeThroughCommandAdapter(sessionId));
        assertEquals(5, getSizeThroughCommandAdapter(restartedSessionId));
        tagMessage("inactive", 5, getMessageIds(sessionId, 5, 0));
        assertEquals(5, getMessagesThroughCommandAdapter(restartedSessionId, 100, 0));
    }

    private void startStorageModule() throws AttributeRegistrationException, URISyntaxException {
        storageModule = new StorageModule();
        storageModule.run(node, getStorageModuleAttributes(node.channels(), STORAGE_COMMAND_URI));
        commandAdapter = new CommandAdapter(storageModule);
        storageProvider = storageModule.getStorageProvider();
    }

    @Test
    public void getCorrectNumberMessagesWithGivenLimitSkip() throws Exception {
        Long sessionId = startSessionSendMessages(5);

        assertEquals(5, getMessagesThroughCommandAdapter(sessionId, 100, 0));
        assertEquals(0, getMessagesThroughCommandAdapter(sessionId, 100, 6));
        assertEquals(0, getMessagesThroughCommandAdapter(sessionId, 0, 0));
        exception.expect(StorageModuleException.class);
        getMessagesThroughCommandAdapter(sessionId, -1, 0);
        exception.expect(StorageModuleException.class);
        getMessagesThroughCommandAdapter(sessionId, 100, -1);
    }

    @Test
    public void throwWhenInvalidSessionInGetMessage() throws Exception {
        startSessionSendMessages(5);
        exception.expect(StorageModuleException.class);
        assertEquals(0, getMessagesThroughCommandAdapter(123L, 100, 0));
    }

    @Test
    public void tagMessageTest() throws Exception {
        Long sessionId = startSessionSendMessages(5);
        tagMessage("inactive", 5, getMessageIds(sessionId, 5, 0));
    }

    @Test
    public void deleteByTagMessageTest() throws Exception {
        Long sessionId = startSessionSendMessages(10);

        tagMessage("inactive", 10, getMessageIds(sessionId, 10, 0));
        sleep(2);
        deleteByTagMessage("inactive", 2L, 10);
    }

    @Test
    public void deleteByTagMessageTestUsingChannelNameTag() throws Exception {
        startSessionSendMessages(10);
        deleteByTagMessage(listenChannelUriString, 0L, 10);
    }

    @Test
    public void deleteByTagMessageTestUsingSessionIdTag() throws Exception {
        Long sessionId = startSessionSendMessages(10);
        deleteByTagMessage(sessionId.toString(), 0L, 10);
    }

    @Test
    public void untagMessageTest() throws Exception {
        Long sessionId = startSessionSendMessages(10);
        long ids[] = getMessageIds(sessionId, 10, 0);

        tagMessage("inactive", 10, ids);
        untagMessage("inactive", 10, ids);
    }

    @Test
    public void listTagsMessageTest() throws Exception {
        Long sessionId1 = startSessionSendMessages(5);
        long ids[] = getMessageIds(sessionId1, 10, 0);
        tagMessage("inactive", 5, ids);
        Set<String> listTagResponse = (Set<String>) new ListTagMessage(listenChannelUriString).onCommandMessage(storageModule);
        assertTrue(listTagResponse.contains(new Tag(sessionId1.toString()).toString()));
        assertTrue(listTagResponse.contains(new Tag(listenChannelUriString).toString()));
        assertTrue(listTagResponse.contains(new Tag("inactive").toString()));

        Long sessionId2 = startSessionSendMessages(5);
        assertEquals(5, getMessagesThroughCommandAdapter(sessionId2, 10, 0));

        Set<String> listTagResponse2 = (Set<String>) new ListTagMessage(listenChannelUriString).onCommandMessage(storageModule);
        assertTrue(listTagResponse2.contains(new Tag(sessionId1.toString()).toString()));
        assertTrue(listTagResponse2.contains(new Tag(sessionId2.toString()).toString()));
        assertTrue(listTagResponse2.contains(new Tag(listenChannelUriString).toString()));
        assertTrue(listTagResponse2.contains(new Tag("inactive").toString()));

        sleep(2);

        deleteByTagMessage("inactive", 2L, 5);
    }

    @Test
    public void testGetSessionsOnlyWithActiveMessages() throws Exception{
        Long sessionId1 = startSessionSendMessages(2);

        List<PersistentMessage> messageList1 = storageModule.getStorageProvider().getMessages(sessionId1, 2, 0);
        assertEquals(2, messageList1.size());

        // rename to a new session
        Long renamedSessionId = renameSessionThroughCommandAdapter(sessionId1);
        sendMessagesToStorage(3);
        assertEquals(3, getMessagesThroughCommandAdapter(renamedSessionId, 5, 0));

        Long renamedSessionId2 = renameSessionThroughCommandAdapter(renamedSessionId);
        tagMessage("inactive", 2, messageList1.stream().mapToLong(PersistentMessage::getId).toArray());

        Set<Collection<Long>> sessionIdSet = storageModule.getStorageProvider().getSessionsWithActiveMessages(renamedSessionId2);
        assertEquals(3, sessionIdSet.iterator().next().size());
        assertFalse(sessionIdSet.iterator().next().contains(sessionId1));
    }

    private long[] getMessageIds(Long sessionId, int limit, int offset) throws Exception {
        List<PersistentMessage> pm = storageModule.getStorageProvider().getMessages(sessionId, limit, offset);
        List<Long> ids = pm.stream().map(PersistentMessage::getId).collect(Collectors.toList());
        return ArrayUtils.toPrimitive(ids.toArray(new Long[ids.size()]));
    }

    private void sleep(int sleepInSeconds) {
        Date end = new Date();
        Date start = new Date();
        while (end.getTime() - start.getTime() < sleepInSeconds * 1000) {
            end = new Date();
        }
    }

    private void deleteByTagMessage(String tag, Long before, int numberOfTaggedMessagesExpected) throws Exception {
        DeleteByTagMessage deleteByTagMsg = new DeleteByTagMessage(new String[] { tag }, before);
        assertEquals(numberOfTaggedMessagesExpected, deleteByTagMsg.onCommandMessage(storageModule));
    }

    private void tagMessage(String tag, int numberOfTaggedMessagesExpected, long ids[]) throws Exception {
        TagMessage tagMsg = new TagMessage(new String[] { tag }, ids);
        assertEquals(numberOfTaggedMessagesExpected, tagMsg.onCommandMessage(storageModule));
    }

    private void untagMessage(String tag, int numberOfTaggedMessagesExpected, long ids[]) throws Exception {
        UntagMessage tagMsg = new UntagMessage(new String[] { tag }, ids);
        assertEquals(numberOfTaggedMessagesExpected, tagMsg.onCommandMessage(storageModule));
    }

    private Long startSession(String listenChannelUriString) throws Exception {
        StartMessage startMsg = new StartMessage(listenChannelUriString);
        Object sessionId = startMsg.onCommandMessage(storageModule);
        return (Long) sessionId;
    }

    private void sendMessagesToStorage(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            listenChannel.publish(generateMessage(i));
        }
    }

    private int getSizeThroughCommandAdapter(Long sessionId) throws Exception {
        Map<String, Object> inputs = new LinkedHashMap<>();
        inputs.put("sessionId", sessionId);
        return (int) commandAdapter.size(inputs);
    }

    private int getMessagesThroughCommandAdapter(Long sessionId, int limit, int skip) throws Exception {
        Map<String, Object> inputs = new LinkedHashMap<>();
        inputs.put("sessionId", sessionId);
        inputs.put("limit", limit);
        inputs.put("skip", skip);
        inputs.put("replayChannel", "uri://replaychannel");
        return (int) commandAdapter.get(inputs);
    }

    private Long renameSessionThroughCommandAdapter(Long sessionId) throws Exception {
        Map<String, Object> inputs = new LinkedHashMap<>();
        inputs.put("sessionId", sessionId);
        return (Long) commandAdapter.rename(inputs);
    }

    private BytesMessage generateMessage(int id) {
        return new BytesMessage((".....#" + id).getBytes());
    }

    private Attributes getStorageModuleAttributes(Channels channels, URI storageModuleUri)
            throws AttributeRegistrationException, URISyntaxException {
        Attributes attributes = AttributesFactory.buildEmptyAttributes(channels, storageModuleUri);
        attributes.add(new IdAttribute(42));
        attributes.add(new AckChannelAttribute("ndn:/intel/storage/acknowledged"));
        attributes.add(new ModuleStateAttribute());
        return attributes;
    }
}