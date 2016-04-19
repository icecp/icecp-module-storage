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

package com.intel.icecp.module.storage.messages;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intel.icecp.core.Channel;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.InconsistentStateException;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleteSessionTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    private StorageModule mockModule;
    @Mock
    private StorageProvider mockProvider;
    @Mock
    private PersistCallback mockPersistCallback;
    @Mock
    private Channel<BytesMessage> mockChannel;
    private Long SESSION_ID = 123456789L;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void throwWhenNullSessionId() throws StorageModuleException {
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        DeleteSession msg = getDeleteMessage(null);

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    private void setupMocks(Long sessionId, Long renamedSessionId) throws Exception {
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        when(mockProvider.getPreviousSession(sessionId)).thenReturn(renamedSessionId);
    }

    @Test
    public void throwWhenDeleteFails() throws Exception {
        Long attachedSessionId = 0L;
        setupMocks(SESSION_ID, attachedSessionId);

        doThrow(StorageModuleException.class).when(mockProvider).deleteSession(SESSION_ID);

        DeleteSession msg = getDeleteMessage(SESSION_ID);

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);

        //verify the test did make it all the way to the delete messages API
        verify(mockProvider, times(1)).deleteSession(SESSION_ID);
    }

    @Test
    public void testLinkedVertexMethodIsCalled() throws Exception {
        Long attachedSessionId = 0L;
        setupMocks(SESSION_ID, attachedSessionId);

        when(mockModule.getCallback(SESSION_ID)).thenReturn(mockPersistCallback);
        when(mockModule.getChannel(SESSION_ID)).thenReturn(Optional.of(mockChannel));

        DeleteSession msg = getDeleteMessage(SESSION_ID);

        msg.onCommandMessage(mockModule);

        verify(mockProvider, times(1)).getPreviousSession(SESSION_ID);
    }

    @Test
    public void testNullCallbackReturned() throws Exception {
        Long attachedSessionId = 0L;
        setupMocks(SESSION_ID, attachedSessionId);

        when(mockModule.getCallback(SESSION_ID)).thenReturn(null);
        doThrow(StorageModuleException.class).when(mockProvider).deleteSession(SESSION_ID);

        DeleteSession msg = getDeleteMessage(SESSION_ID);

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void testUpdateSubscriptionCallbackIsCalled() throws Exception {
        Long attachedSessionId = 100L;
        setupMocks(SESSION_ID, attachedSessionId);

        when(mockModule.getCallback(SESSION_ID)).thenReturn(mockPersistCallback);
        when(mockModule.getChannel(SESSION_ID)).thenReturn(Optional.of(mockChannel));

        DeleteSession msg = getDeleteMessage(SESSION_ID);

        msg.onCommandMessage(mockModule);

        verify(mockPersistCallback, times(1)).setSessionId(attachedSessionId);
    }

    @Test
    public void throwsInconsistentStateExceptionDeleteSession() throws Exception {
        Long attachedSessionId = 0L;
        setupMocks(SESSION_ID, attachedSessionId);

        doThrow(InconsistentStateException.class).when(mockProvider).deleteSession(SESSION_ID);

        DeleteSession msg = getDeleteMessage(SESSION_ID);

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void deleteDeleteSessionSuccessfully() throws Exception {
        Long attachedSessionId = 0L;
        setupMocks(SESSION_ID, attachedSessionId);

        when(mockModule.getCallback(SESSION_ID)).thenReturn(mockPersistCallback);
        when(mockModule.getChannel(SESSION_ID)).thenReturn(Optional.of(mockChannel));

        DeleteSession msg = getDeleteMessage(SESSION_ID);

        msg.onCommandMessage(mockModule);

        //verify the test did make it all the way to the delete messages API
        verify(mockProvider, times(1)).deleteSession(SESSION_ID);
    }

    @Test
    public void throwsExceptionWhenCallbackNotFoundForActiveSession() throws Exception {
        Long attachedSessionId = 0L;
        setupMocks(SESSION_ID, attachedSessionId);

        when(mockModule.getCallback(SESSION_ID)).thenReturn(null);
        when(mockModule.getChannel(SESSION_ID)).thenReturn(Optional.of(mockChannel));

        DeleteSession msg = getDeleteMessage(SESSION_ID);

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void throwsExceptionWhenChannelNotFoundForActiveSession() throws Exception {
        Long attachedSessionId = 0L;
        setupMocks(SESSION_ID, attachedSessionId);

        when(mockModule.getCallback(SESSION_ID)).thenReturn(mockPersistCallback);
        when(mockModule.getChannel(SESSION_ID)).thenReturn(Optional.empty());

        DeleteSession msg = getDeleteMessage(SESSION_ID);

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);

    }

    @Test
    public void testDeleteSessionInactiveSession() throws Exception {
        Long attachedSessionId = 0L;
        setupMocks(SESSION_ID, attachedSessionId);

        when(mockModule.getCallback(SESSION_ID)).thenReturn(null);
        when(mockModule.getChannel(SESSION_ID)).thenReturn(Optional.empty());

        DeleteSession msg = getDeleteMessage(SESSION_ID);

        msg.onCommandMessage(mockModule);

    }

    @Test
    public void testSerialization() throws IOException {
        String cmd = "{\"@cmd\" : \"DELETE_SESSION\", \"sessionId\" : null}";
        ObjectMapper mapper = new ObjectMapper();

        DeleteSession msg = mapper.readValue(cmd, DeleteSession.class);
        System.out.println(msg.toString());
    }

    @Test
    public void throwWhenInconsistentStateExceptionCaught() throws Exception {
        Long attachedSessionId = 0L;
        setupMocks(SESSION_ID, attachedSessionId);

        doThrow(InconsistentStateException.class).when(mockProvider).deleteSession(SESSION_ID);

        DeleteSession msg = new DeleteSession(SESSION_ID);
        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void toStringReturnsCorrectly() throws IOException {
        DeleteSession msg = new DeleteSession(1L);
        String expected = "StorageDeleteSession{session ID=1} BaseMessage{" + "cmd=DELETE_SESSION}";
        assertEquals(expected, msg.toString());
    }

    private DeleteSession getDeleteMessage(Long sessionId) {
        return new DeleteSession(sessionId);
    }
}
