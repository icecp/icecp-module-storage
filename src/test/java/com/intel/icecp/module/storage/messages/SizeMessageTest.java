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