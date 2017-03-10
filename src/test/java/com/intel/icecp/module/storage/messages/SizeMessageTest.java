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
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.when;


public class SizeMessageTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    private StorageModule mockModule;
    @Mock
    private StorageProvider mockProvider;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void throwWhenSessionIsNull() throws StorageModuleException {
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);

        exception.expect(StorageModuleException.class);
        (new SizeMessage(null)).onCommandMessage(mockModule);
    }

    @Test
    public void returnCorrectSizeWhenSessionHasMessages() throws StorageModuleException {
        int msgCount = 99;
        long sessionId = 123L;
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        when(mockProvider.getSessionSize(sessionId)).thenReturn(msgCount);

        SizeMessage msg = new SizeMessage(sessionId);
        Object resp = msg.onCommandMessage(mockModule);

        //verify the count is correct
        assertEquals(msgCount, resp);
    }
}