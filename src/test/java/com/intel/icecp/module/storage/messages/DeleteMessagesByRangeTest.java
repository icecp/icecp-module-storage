package com.intel.icecp.module.storage.messages;

import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.InconsistentStateException;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

/**
 * Created by nmgaston on 7/12/2016.
 */
public class DeleteMessagesByRangeTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    private StorageModule mockModule;
    @Mock
    private StorageProvider mockProvider;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void throwWhenNullSessionId() throws StorageModuleException {
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        DeleteMessagesByRange msg = new DeleteMessagesByRange(null, 0, 3);

        exception.expect(StorageModuleException.class);
        msg.onCommandMessage(mockModule);
    }

    @Test
    public void deleteMessagesByRangeSuccessfully() throws InconsistentStateException, StorageModuleException {
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        doNothing().when(mockProvider).deleteMessagesByRange(Mockito.anyLong(), Mockito.anyInt(), Mockito.anyInt());

        DeleteMessagesByRange msg = new DeleteMessagesByRange(1L, 0, 3);
        assertTrue((Boolean) msg.onCommandMessage(mockModule));
    }

}
