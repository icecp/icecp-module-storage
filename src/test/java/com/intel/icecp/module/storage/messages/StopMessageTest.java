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

import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

public class StopMessageTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    private StorageModule mockModule;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void throwWhenSessionIdIsNull() throws StorageModuleException {
        StopMessage msg = new StopMessage(null);
        Mockito.doThrow(new StorageModuleException("Received a null sessionId")).when(mockModule).stopSessionChannel(null);
        
        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void throwWhenRemoveFails() throws StorageModuleException {
        Long sessionId = 52L;
        Mockito.doThrow(new StorageModuleException("failed to stop Session")).when(mockModule).stopSessionChannel(sessionId);
        when(mockModule.removeChannel(sessionId)).thenReturn(false);
        StopMessage msg = new StopMessage(sessionId);

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void stopMessagesSuccessfully() throws StorageModuleException {
        Long sessionId = 52L;
        when(mockModule.removeChannel(sessionId)).thenReturn(true);
        when(mockModule.closeChannel(sessionId)).thenReturn(true);
        when(mockModule.removeSubscriptionCallback(sessionId)).thenReturn(true);
        
        StopMessage msg = new StopMessage(sessionId);

        msg.onCommandMessage(mockModule);
    }
}