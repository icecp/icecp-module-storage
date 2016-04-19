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
