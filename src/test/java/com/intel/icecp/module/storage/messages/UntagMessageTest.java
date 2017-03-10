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

import com.intel.icecp.module.query.Id;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 *  Unit tests for Untag Message command
 *
 *
 */
public class UntagMessageTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    StorageModule mockStorageModule;
    @Mock
    StorageProvider mockStorageProvider;

    private long ids[] = {1L, 2L, 3L};
    private String tags[] = {"abc", "def", "ghi"};

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(mockStorageModule.getStorageProvider()).thenReturn(mockStorageProvider);
    }

    @Test
    public void throwWhenTagNameIsEmpty() throws Exception {
        exception.expect(StorageModuleException.class);
        new UntagMessage(null, ids).onCommandMessage(mockStorageModule);
    }

    @Test
    public void throwWhenIdIsNull() throws Exception {
        exception.expect(StorageModuleException.class);
        new UntagMessage(tags, null).onCommandMessage(mockStorageModule);
    }

    @Test
    public void returnsTaggedVertex() throws Exception {
        Set<Id> returnedTaggedIds = new HashSet<>(Arrays.asList(new Id(1), new Id(2), new Id(3)));
        when(mockStorageProvider.untag(any(),any())).thenReturn(returnedTaggedIds);

        int vertexSize = (int) new UntagMessage(tags, ids).onCommandMessage(mockStorageModule);
        assertEquals(ids.length, vertexSize);
    }
}
