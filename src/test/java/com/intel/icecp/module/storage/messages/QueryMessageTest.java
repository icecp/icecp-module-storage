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

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class QueryMessageTest {

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
    public void onCommandWhenQueryInvalidSessionId() {
        final long invalidSessionId = 1234L;
        QueryMessage msg = new QueryMessage(invalidSessionId);
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        try {
            msg.onCommandMessage(mockModule);
        } catch (StorageModuleException e) {
            String expectedErrMsg = "Invalid querySessionId = " + invalidSessionId;
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void onCommandWhenQueryChannelInvalidURI() {
        String invalidQueryChannel = "foobar.uri\\";
        QueryMessage msg = new QueryMessage(invalidQueryChannel);
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        try {
            msg.onCommandMessage(mockModule);
        } catch (StorageModuleException e) {
            assertTrue(e.getMessage().contains("Invalid queryChannel URI syntax, queryChannel ="));
        }
    }

    @Test
    public void returnCorrectSessionWhenQueryChannelFindsSessions() throws StorageModuleException {
        String queryChannel = "uri://querychannel";
        QueryMessage msg = new QueryMessage(queryChannel);

        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        Set<Collection<Long>> sessionSet = new HashSet<>();
        Collection<Long> sessionCollection = new LinkedList<>();
        sessionCollection.add(3L);
        sessionCollection.add(5L);
        sessionSet.add(sessionCollection);
        when(mockProvider.getSessions(any(URI.class))).thenReturn(sessionSet);

        Object resp = msg.onCommandMessage(mockModule);
        // verify the session set is what was expected
        assertEquals(sessionSet, resp);
    }

    @Test
    public void onCommandWhenQuerySessionIdFindsSessions() throws Exception {
        Long querySessionId = 1234L;
        QueryMessage msg = new QueryMessage(querySessionId);

        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        Set<Collection<Long>> sessionSet = new HashSet<>();
        Collection<Long> sessionCollection = new LinkedList<>();
        sessionCollection.add(3L);
        sessionCollection.add(5L);
        sessionSet.add(sessionCollection);
        when(mockProvider.getSessions(any(Long.class))).thenReturn(sessionSet);

        Object resp = msg.onCommandMessage(mockModule);
        // verify the session set is what was expected
        assertEquals(sessionSet, resp);

    }
}