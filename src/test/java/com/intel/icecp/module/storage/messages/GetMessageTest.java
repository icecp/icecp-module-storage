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
import com.intel.icecp.core.misc.ChannelIOException;
import com.intel.icecp.core.misc.ChannelLifetimeException;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GetMessageTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    private StorageModule mockModule;
    @Mock
    private StorageProvider mockProvider;
    @Mock
    private Node mockNode;
    @Mock
    private Channel<BytesMessage> mockChannel;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void throwWhenSessionIdIsNull() throws StorageModuleException {
        GetMessage msg = new GetMessage(null, 0, 0, "uri://replaychannel");

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void throwWhenReplayChannelIsNull() throws StorageModuleException {
        GetMessage msg = new GetMessage(12L, 0, 0, null);

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void throwWhenReplayChannelIsEmpty() throws StorageModuleException {
        GetMessage msg = new GetMessage(12L, 0, 0, "");

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void throwsOnCommandWithCreateChannelException() throws Exception {
        Long sessionId = 100L;
        createMockModule(sessionId);
        when(mockNode.openChannel(any(URI.class), any(), any(Persistence.class)))
                .thenThrow(new ChannelLifetimeException("mockexception"));
        GetMessage msg = new GetMessage(sessionId, 0, 0, "uri://replaychannel");

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void callOnCommandWithEmptySessionSuccessfully() throws Exception {
        Long sessionId = 100L;
        int limit = 10;
        int offset = 0;
        createMockModule(sessionId);
        Mockito.<Channel<BytesMessage>>when(mockNode.openChannel(any(URI.class), any(), any(Persistence.class)))
                .thenReturn(mockChannel);
        when(mockProvider.getMessages(sessionId, limit, offset)).thenReturn(new ArrayList<>());

        GetMessage msg = new GetMessage(sessionId, limit, offset, "uri://replaychannel");

        msg.onCommandMessage(mockModule);

        // verify that everything is OK, but nothing was ever published.
        verify(mockChannel, never()).publish(any(BytesMessage.class));
    }

    @Test
    public void callOnCommandWithSessionElementsReturnedSuccessfully() throws Exception {
        Long sessionId = 100L;
        int limit = 10;
        int offset = 0;
        createMockModule(sessionId);
        List<PersistentMessage> messages = new ArrayList<>();
        messages.add(new PersistentMessage(1, System.currentTimeMillis(), new byte[0]));
        messages.add(new PersistentMessage(1, System.currentTimeMillis(), new byte[0]));
        messages.add(new PersistentMessage(1, System.currentTimeMillis(), new byte[0]));
        Mockito.<Channel<BytesMessage>>when(mockNode.openChannel(any(URI.class), any(), any(Persistence.class)))
                .thenReturn(mockChannel);
        when(mockProvider.getMessages(sessionId, limit, offset)).thenReturn(messages);

        GetMessage msg = new GetMessage(sessionId, limit, offset, "uri://replaychannel");

        msg.onCommandMessage(mockModule);

        // verify that everything is OK, and number of publish calls == msgs
        // returned from the provider
        verify(mockChannel, times(messages.size())).publish(any(BytesMessage.class));
    }

    @Test
    public void throwsWhenPublishFails() throws Exception {
        Long sessionId = 100L;
        int limit = 10;
        int offset = 0;
        createMockModule(sessionId);
        List<PersistentMessage> messages = new ArrayList<>();
        messages.add(new PersistentMessage(1, System.currentTimeMillis(), new byte[0]));
        messages.add(new PersistentMessage(1, System.currentTimeMillis(), new byte[0]));
        messages.add(new PersistentMessage(1, System.currentTimeMillis(), new byte[0]));
        Mockito.<Channel<BytesMessage>>when(mockNode.openChannel(any(URI.class), any(), any(Persistence.class)))
                .thenReturn(mockChannel);
        when(mockProvider.getMessages(sessionId, limit, offset)).thenReturn(messages);
        // mock up so that an exception is thrown on the second publish
        doNothing().doThrow(new ChannelIOException("mockexception")).when(mockChannel).publish(any());

        GetMessage msg = new GetMessage(sessionId, limit, offset, "uri://replaychannel");

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);

        // verify that it reports a channel error on the second publish
        verify(mockChannel, times(2)).publish(any(BytesMessage.class));
    }

    @Test
    public void throwWhenBadReplayURI() throws Exception {
        Long sessionId = 100L;
        int limit = 10;
        int offset = 0;
        createMockModule(sessionId);

        GetMessage msg = new GetMessage(sessionId, limit, offset, "uri:\\replaychannel");

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void getMessagesWhenNegativeLimitValue() throws Exception {
        Long sessionId = 100L;
        int limit = -1;
        int offset = 0;

        createMockModule(sessionId);
        when(mockProvider.getSessionSize(sessionId)).thenReturn(2);

        GetMessage msg = new GetMessage(sessionId, limit, offset, "uri://replaychannel");
        assertEquals(new Integer(-1), msg.getLimit());

        try {
            msg.onCommandMessage(mockModule);
        } catch (StorageModuleException e) {
            assertTrue(e.getMessage().contains("Invalid values for limit ="));
        }

    }

    @Test
    public void getMessagesWhenNegativeSkipValues() throws Exception {
        Long sessionId = 100L;
        int limit = 10;
        Integer offset = -1;
        createMockModule(sessionId);

        GetMessage msg = new GetMessage(sessionId, limit, offset, "uri://replaychannel");
        assertEquals(new Integer(-1), msg.getSkip());

        try {
            msg.onCommandMessage(mockModule);
        } catch (StorageModuleException e) {
            assertTrue(e.getMessage().contains("Invalid values for limit ="));
        }

    }

    @Test
    public void getMessagesWhenLimitAndSkipValuesNull() throws Exception {
        Long sessionId = 100L;
        Integer limit = null;
        Integer offset = null;
        createMockModule(sessionId);
        when(mockProvider.getSessionSize(sessionId)).thenReturn(2);

        GetMessage msg = new GetMessage(sessionId, limit, offset, "uri://replaychannel");
        assertNull(msg.getLimit());
        assertNull(msg.getSkip());

        try {
            msg.onCommandMessage(mockModule);
        } catch (StorageModuleException e) {
            assertTrue(e.getMessage().contains("Null values for limit ="));
        }
    }

    private void createMockModule(long mockSessionId) throws Exception {
        when(mockProvider.createSession(any())).thenReturn(mockSessionId);
        when(mockModule.getNode()).thenReturn(mockNode);
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
    }
}
