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