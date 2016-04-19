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