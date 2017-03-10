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
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.exceptions.TaggingOperationException;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ListTag command
 *
 */
public class ListTagMessageTest {
    private static final String TAG_TEST_CHANNEL = "ndn:/intel/test/taglist";
    private static final Tag TEST_CHANNEL_TAG = new Tag(TAG_TEST_CHANNEL);

    private ListTagMessage listTagMessage;
    private Set<Tag> returnedChannelTagSet;
    private Set<String> actualReturnSet;

    @Mock
    StorageModule mockStorageModule;
    @Mock
    StorageProvider mockStorageProvider;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        listTagMessage = new ListTagMessage(TAG_TEST_CHANNEL);
        when(mockStorageModule.getStorageProvider()).thenReturn(mockStorageProvider);
        returnedChannelTagSet = new LinkedHashSet<>();
    }

    @Test
    public void testConstructorTagReplyChannel() {
        assertEquals(TAG_TEST_CHANNEL, listTagMessage.getQuery());
    }

    @Test(expected = StorageModuleException.class)
    public void throwExceptionWhenTagChannelIsNull() throws Exception {
        new ListTagMessage(null).onCommandMessage(mockStorageModule);
    }

    @Test(expected = StorageModuleException.class)
    public void throwExceptionWhenTagChannelIsEmpty() throws Exception {
        new ListTagMessage("").onCommandMessage(mockStorageModule);
    }

    @Test(expected = StorageModuleException.class)
    public void throwExceptionWhenTagChannelIsNotWellFormedURI() throws Exception {
        new ListTagMessage("JUNK_URI:\\illed-formed.uri").onCommandMessage(mockStorageModule);
    }

    @Test
    public void testToString() {
        assertTrue(listTagMessage.toString().contains("ListTagMessage{query"));
    }

    @Test
    public void testReturnCorrectListTagsWhenTagExists() throws Exception {
        generateSampleTagSet();
        when(mockStorageProvider.related(TEST_CHANNEL_TAG)).thenReturn(returnedChannelTagSet);

        Set<String> res = (Set<String>) listTagMessage.onCommandMessage(mockStorageModule);

        assertTrue(res instanceof Set);
        assertTrue(res.contains("Tag{Id123}"));
        assertTrue(res.contains("Tag{Id456}"));
        assertTrue(res.contains("Tag{Id789}"));
    }

    @Test
    public void testReturnEmptyListTagWhenTagNotExists() throws Exception {
        generateSampleTagSet();
        when(mockStorageProvider.related(TEST_CHANNEL_TAG)).thenReturn(returnedChannelTagSet);

        Set<String> res = (Set<String>) new ListTagMessage("Non-Existing-Channel-Tag").onCommandMessage(mockStorageModule);

        assertTrue(res instanceof Set);
        assertNotEquals(returnedChannelTagSet, res);
        assertTrue(res.isEmpty());
    }

    @Test
    public void testThrowTagOperationExceptionWhenListTagFails() throws Exception {
        when(mockStorageProvider.related(TEST_CHANNEL_TAG)).thenReturn(returnedChannelTagSet);
        doThrow(new TaggingOperationException("failedToListTags")).when(mockStorageProvider).related(any(Tag.class));

        try {
            listTagMessage.onCommandMessage(mockStorageModule);
            fail("Expect the exception!");
        } catch (StorageModuleException e) {
            assertTrue(e.getMessage().contains("Failed to list tags for query"));
        }
    }

    private void generateSampleTagSet() {
        actualReturnSet = new HashSet<>();
        actualReturnSet.add("Id123");
        actualReturnSet.add("Id456");
        actualReturnSet.add("Id789");

        returnedChannelTagSet.clear();

        actualReturnSet.forEach(tag -> returnedChannelTagSet.add(new Tag(tag)));
    }

}