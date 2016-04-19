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

import com.intel.icecp.core.Channel;
import com.intel.icecp.core.messages.BytesMessage;
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
import java.net.URISyntaxException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RenameMessageTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private URI mockURI;
    @Mock
    private StorageModule mockModule;
    @Mock
    private Channel<BytesMessage> mockChannel;
    @Mock
    private StorageProvider mockProvider;
    @Mock
    private PersistCallback mockPersistCallback;
    private Long SESSION_ID = 123456789L;
    private Long NEW_SESSION_ID = 9876543210L;

    @Before
    public void before() throws URISyntaxException {
        MockitoAnnotations.initMocks(this);
        mockURI = new URI("test://testuri");
    }

    @Test
    public void throwWhenSessionIdIsNull() throws StorageModuleException {
        exception.expect(StorageModuleException.class);
        (new RenameMessage(null)).onCommandMessage(mockModule);
    }

    @Test
    public void throwWhenSessionIdNotFound() throws StorageModuleException {
        when(mockModule.getChannel(SESSION_ID)).thenReturn(Optional.empty());

        exception.expect(StorageModuleException.class);
        (new RenameMessage(SESSION_ID)).onCommandMessage(mockModule);
    }

    @Test
    public void renameSessionSuccess() throws Exception {
        setUpMocks(SESSION_ID);
        when(mockProvider.renameSession(mockChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);

        RenameMessage msg = new RenameMessage(SESSION_ID);

        Long newSessionId = (Long) msg.onCommandMessage(mockModule);
        assertNotEquals(newSessionId, SESSION_ID);
    }

    @Test
    public void testUpdateSubscriptionCallbackIsCalled() throws Exception {
        setUpMocks(SESSION_ID);
        when(mockProvider.renameSession(mockChannel.getName(), SESSION_ID)).thenReturn(NEW_SESSION_ID);

        RenameMessage msg = new RenameMessage(SESSION_ID);

        Long newSessionId = (Long) msg.onCommandMessage(mockModule);
        assertNotEquals(newSessionId, SESSION_ID);

        verify(mockPersistCallback, times(1)).setSessionId(NEW_SESSION_ID);
    }

    @Test
    public void returnErrorResponseWhenInconsistentStateException() throws Exception {
        setUpMocks(SESSION_ID);
        when(mockProvider.renameSession(mockChannel.getName(), SESSION_ID)).thenThrow(StorageModuleException.class);

        exception.expect(StorageModuleException.class);
        (new RenameMessage(SESSION_ID)).onCommandMessage(mockModule);
    }

    private void setUpMocks(Long sessionId) {
        when(mockModule.getChannel(sessionId)).thenReturn(Optional.of(mockChannel));
        when(mockChannel.getName()).thenReturn(mockURI);
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        when(mockModule.getCallback(sessionId)).thenReturn(mockPersistCallback);
    }
}