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