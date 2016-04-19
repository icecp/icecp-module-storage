package com.intel.icecp.module.storage.messages;

import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.InconsistentStateException;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Created by Natalie Gaston, natalie.gaston@intel.com on 6/22/2016.
 */
public class CommandAdapterTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    private StorageModule mockModule;
    @Mock
    private StorageProvider mockProvider;
    private CommandAdapter commandAdapter;


    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(mockModule.getStorageProvider()).thenReturn(mockProvider);
        commandAdapter = new CommandAdapter(mockModule);
    }

    private Map<String, Object> createMapForTagTests() {
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("ids", new ArrayList<>(Arrays.asList(1L, 2L, 3L)));
        inputMap.put("tags", new ArrayList<>(Arrays.asList("a", "b", "c")));
        return inputMap;
    }
    
    @Test
    public void returnCorrectSetForGetRequiredStringSetParameter() throws Exception {
        Map<String, Object> inputMap = createMapForTagTests();

        String[] expectedElements = new String[]{"a","b","c"};
        ArrayList<String> returnedTags = (ArrayList<String>) commandAdapter.getRequiredSetParameter("tags", inputMap);
        String[] tags = returnedTags.toArray(new String[returnedTags.size()]);
        assertTrue(Arrays.equals(expectedElements, tags));
    }

    @Test
    public void returnCorrectSetForGetRequiredLongSetParameter() throws Exception {
        Map<String, Object> inputMap = createMapForTagTests();

        Long[] expectedElements = new Long[] { 1L, 2L, 3L };
        ArrayList<Long> returnedIds = (ArrayList<Long>) commandAdapter.getRequiredSetParameter("ids", inputMap);
        Long[] ids = returnedIds.toArray(new Long[returnedIds.size()]);
        assertTrue(Arrays.equals(expectedElements, ids));
    }

    @Test
    public void throwWhenInputMapIsNull() throws Exception {
        exception.expect(StorageModuleException.class);
        commandAdapter.getRequiredSetParameter("tags", null);
    }

    @Test
    public void throwWhenUnableToFindTag() throws Exception {
        Map<String, Object> inputMap = createMapForTagTests();

        exception.expect(StorageModuleException.class);
        commandAdapter.getRequiredSetParameter("tags1", inputMap);
    }

    @Test
    public void returnListTagType() throws StorageModuleException {
        Map<String, Object> inputs = new LinkedHashMap<>();
        inputs.put("query", "ndn:/intel/test/channel");
        Object returnObj = commandAdapter.listTag(inputs);
        assertNotNull(returnObj);
        assertTrue(returnObj instanceof Set);
    }
}
