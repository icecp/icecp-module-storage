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

import com.intel.icecp.core.Node;
import com.intel.icecp.module.query.Query;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for DeleteByTag command
 *
 */
public class DeleteByTagMessageTest {

    @Mock
    Node mockNode;
    @Mock
    StorageModule mockStorageModule;
    @Mock
    StorageProvider mockStorageProvider;
    private Set<PersistentMessage> deletedMessages;

    @Before
    public void before() throws Exception {
        MockitoAnnotations.initMocks(this);
        deletedMessages = createdDeletedMessagesList();
        when(mockStorageModule.getStorageProvider()).thenReturn(mockStorageProvider);
    }

    @Test(expected = StorageModuleException.class)
    public void NullTagInputTest() throws Exception {
        new DeleteByTagMessage(null, 1234L).onCommandMessage(mockStorageModule);
    }

    @Test(expected = StorageModuleException.class)
    public void EmptyTagInputTest() throws Exception {
        String[] tags = {};
        new DeleteByTagMessage(tags, 1234L).onCommandMessage(mockStorageModule);
    }

    @Test(expected = StorageModuleException.class)
    public void NullTimestampInputTest() throws Exception {
        new DeleteByTagMessage(new String[]{"a", "b"}, null).onCommandMessage(mockStorageModule);
    }

    @Test
    public void DeleteByTagsTest() throws Exception {
        when(mockStorageProvider.remove(any(Query.class))).thenReturn(deletedMessages);
        Object response = new DeleteByTagMessage(new String[]{"a", "b"}, 1234L).onCommandMessage(mockStorageModule);
        assertEquals(5, response);
    }

    private LinkedHashSet<PersistentMessage> createdDeletedMessagesList() {
        LinkedHashSet<PersistentMessage> deleted = new LinkedHashSet<>();
        for (int i = 0; i < 5; i++) {
            deleted.add(new PersistentMessage(i, 1234L, ("..." + i).getBytes()));
        }
        return deleted;
    }

}