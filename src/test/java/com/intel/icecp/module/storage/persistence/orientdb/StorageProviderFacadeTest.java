package com.intel.icecp.module.storage.persistence.orientdb;

import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.module.query.Query;
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.intel.icecp.module.storage.persistence.providers.LegacyStorageProvider;
import com.intel.icecp.module.storage.persistence.providers.TaggedStorageProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.net.URI;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 */
public class StorageProviderFacadeTest {
    private static final String TEST_INPUT = "foo";
    private StorageProviderFacade provider, facadeSpy;
    private Query query;
    private Tag tag;
    private URI testUri;
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    private LegacyStorageProvider legacyStorageProvider;
    @Mock
    private TaggedStorageProvider taggedStorageProvider;
    @Mock
    private PersistentMessage mockMessage;

    @Before
    public void before() throws Exception {
        MockitoAnnotations.initMocks(this);

        facadeSpy = Mockito.spy(new StorageProviderFacade());
        provider = new StorageProviderFacade(createTestOrientDbConfiguration());
        query = new Query();
        tag = new Tag(TEST_INPUT);
        testUri = new URI(TEST_INPUT);
    }

    @After
    public void tearDown() throws Exception {
        provider.shutdown();
    }

    @Test
    public void testFacadeCallsRealMethodMaxTs() throws Exception {
        StorageProviderFacade facadeSpy = Mockito.spy(new StorageProviderFacade());
        facadeSpy.getActiveMaximumTimestamp(TEST_INPUT);
        verify(facadeSpy, times(1)).getActiveMaximumTimestamp(TEST_INPUT);
    }

    @Test
    public void testFacadeCallsRealMethodMinTs() throws Exception {
        facadeSpy.getActiveMinimumTimestamp(TEST_INPUT);
        verify(facadeSpy, times(1)).getActiveMinimumTimestamp(TEST_INPUT);
    }

    @Test
    public void testFacadeCallsRealMethodAdd() throws Exception {
        BytesMessage msg = new BytesMessage(TEST_INPUT.getBytes());
        facadeSpy.add(msg);
        verify(facadeSpy, times(1)).add(msg);
    }

    @Test
    public void testFacadeCallsRealMethodTag() throws Exception {
        facadeSpy.tag(query, tag);
        verify(facadeSpy, times(1)).tag(query, tag);
    }

    @Test
    public void testFacadeCallsRealMethodUnTag() throws Exception {
        facadeSpy.untag(query, tag);
        verify(facadeSpy, times(1)).untag(query, tag);
    }

    @Test
    public void testFacadeCallsRealMethodFind() throws Exception {
        StorageProviderFacade facadeSpy = Mockito.spy(new StorageProviderFacade());
        facadeSpy.find(query);
        verify(facadeSpy, times(1)).find(query);
    }

    @Test
    public void testFacadeCallsRealMethodRelated() throws Exception {
        facadeSpy.related(tag);
        verify(facadeSpy, times(1)).related(tag);
    }

    @Test
    public void testFacadeCallsRealMethodCreateSession() throws Exception {
        facadeSpy.createSession(testUri);
        verify(facadeSpy, times(1)).createSession(testUri);
    }

    @Test
    public void testFacadeCallsRealMethodDeleteSession() throws Exception {
        exception.expect(StorageModuleException.class);
        facadeSpy.deleteSession(100L);
        verify(facadeSpy, times(1)).deleteSession(100L);
    }

    @Test
    public void testFacadeCallsRealMethodSave() throws Exception {
        exception.expect(StorageModuleException.class);
        facadeSpy.saveMessage(100L, mockMessage);
        verify(facadeSpy, times(1)).saveMessage(100L, mockMessage);
    }

    @Test
    public void testFacadeCallsRealMethodGetSessions() throws Exception {
        facadeSpy.getSessions(100L);
        verify(facadeSpy, times(1)).getSessions(100L);
    }

    @Test
    public void testFacadeCallsRealMethodGetSessionSize() throws Exception {
        facadeSpy.getSessionSize(100L);
        verify(facadeSpy, times(1)).getSessionSize(100L);
    }

    @Test
    public void testFacadeCallsRealMethodGetMessages() throws Exception {
        exception.expect(StorageModuleException.class);
        facadeSpy.getMessages(100L);
        verify(facadeSpy, times(1)).getMessages(100L);
    }

    @Test
    public void testDefaultConstructor() {
        provider = new StorageProviderFacade();
        assertNotNull(provider);
    }

    @Test
    public void testConstructorWithConfiguration() throws Exception {
        assertNotNull(provider);
    }

    private OrientDbConfiguration createTestOrientDbConfiguration() {
        OrientDbConfiguration orientDbConfiguration = new OrientDbConfiguration();
        orientDbConfiguration.setStorageType(OrientDbStorageType.IN_MEMORY_GRAPH);
        return orientDbConfiguration;
    }
}