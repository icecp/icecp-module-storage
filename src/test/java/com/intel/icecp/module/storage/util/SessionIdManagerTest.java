package com.intel.icecp.module.storage.util;

import com.intel.icecp.core.Channel;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.messages.PersistCallback;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SessionIdManagerTest {

    @Mock
    StorageModule mockModule;
    @Mock
    Channel<BytesMessage> mockChannel;
    @Mock
    PersistCallback mockCallback;

    private Long SESSION_ID = 77L;
    private Long NEW_SESSION_ID = 88L;
    private SessionIdManager sessionIdManager;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        sessionIdManager = new SessionIdManager(mockModule, SESSION_ID);
    }

    @Test
    public void updateSessionIdsCallsSetSessionId() {
        sessionIdManager.updateNewSessionId(NEW_SESSION_ID, mockChannel, mockCallback);

        verify(mockCallback, times(1)).setSessionId(NEW_SESSION_ID);
    }

    @Test
    public void updateSessionIdsCallsUpdateChannelsMap() {
        sessionIdManager.updateNewSessionId(NEW_SESSION_ID, mockChannel, mockCallback);

        verify(mockModule, times(1)).addChannel(NEW_SESSION_ID, mockChannel, mockCallback);
    }

    @Test
    public void updateSessionIdsCallsUpdateCallbacksMap() {
        sessionIdManager.updateNewSessionId(NEW_SESSION_ID, mockChannel, mockCallback);

        verify(mockModule, times(1)).addChannel(NEW_SESSION_ID, mockChannel, mockCallback);
    }

    @Test
    public void cleanupSessionIdsIsCalled() {
        sessionIdManager.cleanupSessionId();

        verify(mockModule, times(1)).removeChannel(SESSION_ID);
        verify(mockModule, times(1)).removeSubscriptionCallback(SESSION_ID);
    }

}
