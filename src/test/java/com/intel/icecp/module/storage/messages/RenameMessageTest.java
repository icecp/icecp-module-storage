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
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RenameMessageTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private URI mockURI;
    @Mock
    private StorageModule mockModule;
    @Mock
    private Channel<BytesMessage> mockChannel;
    @Mock
    private StorageProvider mockProvider;
    @Mock
    private PersistCallback mockPersistCallback;
    private Long SESSION_ID = 123456789L;
    private Long NEW_SESSION_ID = 9876543210L;

    @Before
    public void before() throws URISyntaxException {
        MockitoAnnotations.initMocks(this);
        mockURI = new URI("test://testuri");
    }

    @Test
    public void throwWhenSessionIdIsNull() throws StorageModuleException {
        exception.expect(StorageModuleException.class);
        (new RenameMessage(null)).onCommandMessage(mockModule);
    }

    @Test
    public void throwWhenSessionIdNotFound() throws StorageModuleException {
        when(mockModule.getChannel(SESSION_ID)).thenReturn(Optional.empty());

        exception.expect(StorageModuleException.class);
        (new RenameMessage(SESSION_ID)).onCommandMessage(mockModule);
    }

    @Test
    public void renameSessionSuccess() throws Exception {
        setUpMocks(SESSION_ID);
        when(mockProvider.renameSession(mockChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);

        RenameMessage msg = new RenameMessage(SESSION_ID);

        Long newSessionId = (Long) msg.onCommandMessage(mockModule);
        assertNotEquals(newSessionId, SESSION_ID);
    }

    @Test
    public void testUpdateSubscriptionCallbackIsCalled() throws Exception {
        setUpMocks(SESSION_ID);
        when(mockProvider.renameSession(mockChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);

        RenameMessage msg = new RenameMessage(SESSION_ID);

        Long newSessionId = (Long) msg.onCommandMessage(mockModule);
        assertNotEquals(newSessionId, SESSION_ID);

        verify(mockPersistCallback, times(1)).setSessionId(NEW_SESSION_ID);
    }

    @Test
    public void returnErrorResponseWhenInconsistentStateException() throws Exception {
        setUpMocks(SESSION_ID);
        when(mockProvider.renameSession(mockChannel.getName(), SESSION_ID)).thenThrow(StorageModuleException.class);

        exception.expect(StorageModuleException.class);
        (new RenameMessage(SESSION_ID)).onCommandMessage(mockModule);
    }

    private void setUpMocks(Long sessionId) {
        when(mockModule.getChannel(sessionId)).thenReturn(Optional.of(mockChannel));
        when(mockChannel.getName()).thenReturn(mockURI);
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        when(mockModule.getCallback(sessionId)).thenReturn(mockPersistCallback);
    }
}