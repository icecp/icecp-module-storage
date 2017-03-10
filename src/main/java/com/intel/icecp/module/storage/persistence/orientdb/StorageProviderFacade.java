/*
 * Copyright (c) 2016 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.icecp.module.storage.persistence.orientdb;

import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.module.query.Id;
import com.intel.icecp.module.query.Query;
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.exceptions.TaggingOperationException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.intel.icecp.module.storage.persistence.providers.LegacyStorageProvider;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import com.intel.icecp.module.storage.persistence.providers.TaggedStorageProvider;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Class which implements specific versions of the provider and proxies calls onto legacy/tagged provider
 * <p>
 * Composite Storage provider for using OrientDb graph database. Currently, it supports
 * both embedded in-memory database and local file persistence. The default
 * graph engine type is in-memory. To configure the local file persistent type,
 * use customized {@link OrientDbConfiguration} as input to the constructor
 * {@link #StorageProviderFacade(OrientDbConfiguration)} like the followings:
 * <br>
 * <blockquote> <code>
 * OrientDbConfiguration orientDbConfiguration = new OrientDbConfiguration();<br>
 * orientDbConfiguration.setStorageType(OrientDbStorageType.EMBEDDED_GRAPH); <br>
 * StorageProviderFacade storageProvider = null;                                     <br>
 * try {                                                                       <br>
 * storageProvider = new StorageProviderFacade(orientDbConfiguration);<br>
 * ... Business logic here ...                                             <br>
 * <br>
 * } catch (Exception e) {                                                <br>
 * e.printStackTrace();                                               <br>
 * } finally {                                                            <br>
 * // clean up the resources                                          <br>
 * ((StorageProviderFacade) storageProvider).shutdown();            <br>
 * }
 * </code> </blockquote> For more details on how to configure OrientDB for local
 * file persistent path, please see {@link OrientDbConfiguration}. <br>
 *
 * @see OrientDbConfiguration
 * @see LegacyStorageProvider
 * @see TaggedStorageProvider
 */
public class StorageProviderFacade implements StorageProvider {
    private static final Logger LOGGER = LogManager.getLogger();

    private final OrientGraph db;
    private final TaggedStorageProvider taggedStorageProvider;
    private final LegacyStorageProvider legacyStorageProvider;
    private static Semaphore lock = new Semaphore(1);
    /**
     * The constructor with the default configuration.
     *
     * @see OrientDbConfiguration
     */
    public StorageProviderFacade() {
        this(new OrientDbConfiguration());
    }

    /**
     * The constructor for instantiate OrientDb storage provider instance with
     * given {@code configuration}. Each provider has its own underlying
     * database connection instance.
     *
     * @param configuration the configuration for OrientDB and cannot be null; otherwise,
     * IllegalArgumentException will be thrown.
     * @see OrientDbConfiguration
     */
    StorageProviderFacade(OrientDbConfiguration configuration) {
        if (configuration != null) {
            synchronized (this) {
                this.db = GraphDbUtils.getGraphDbInstance(configuration);
            }

            // setup all the necessary graph specific schemas
            OrientDbNamespace.setupSchemata(db);

            synchronized (db) {
                legacyStorageProvider = new LegacyOrientDbStorageProvider(db);
                taggedStorageProvider = new TaggedOrientDbStorageProvider(db);
            }

            LOGGER.debug("Storage provider initialized with graph engine: {}", configuration.getStorageType().toString());
        } else {
            throw new IllegalArgumentException("configuration is null!");
        }
    }

    /**
     * Shuts down the current graph database instance to clean up the resources.
     * The shutdown instance cannot be re-used any more.
     */
    public synchronized void shutdown() {
        GraphDbUtils.shutdownDbInstance(db);
    }

    /**
     * Drop the current graph database instance from the disk.
     */
    public synchronized void drop() {
        GraphDbUtils.dropDbInstance(db);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long getActiveMinimumTimestamp(String channelName) throws TaggingOperationException {
        return taggedStorageProvider.getActiveMinimumTimestamp(channelName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long getActiveMaximumTimestamp(String channelName) throws TaggingOperationException {
        return taggedStorageProvider.getActiveMaximumTimestamp(channelName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Id add(BytesMessage message) throws TaggingOperationException {
        return taggedStorageProvider.add(message);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<PersistentMessage> find(Query query) throws TaggingOperationException {
        return taggedStorageProvider.find(query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<PersistentMessage> remove(Query query) throws TaggingOperationException {
        return taggedStorageProvider.remove(query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<Id> tag(Query query, Tag tag) throws TaggingOperationException {
        return taggedStorageProvider.tag(query, tag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<Id> untag(Query query, Tag tag) throws TaggingOperationException {
        return taggedStorageProvider.untag(query, tag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<Tag> related(Tag... tags) throws TaggingOperationException {
        return taggedStorageProvider.related(tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void beginTransaction() throws StorageModuleException {
        try {
            if (!lock.tryAcquire(60, TimeUnit.SECONDS)) {
                throw new StorageModuleException("Failed to acquire lock");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        legacyStorageProvider.beginTransaction();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void commitTransaction() {
        legacyStorageProvider.commitTransaction();
        legacyStorageProvider.commitTransaction();
        lock.release();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void rollbackTransaction() {
        legacyStorageProvider.rollbackTransaction();
        // Check to see if the lock has already been released by a commit.
        if(lock.availablePermits() == 0) {
            lock.release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<URI> getChannels() {
        return legacyStorageProvider.getChannels();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long createSession(URI channelName) throws StorageModuleException {
        return legacyStorageProvider.createSession(channelName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long createSession(URI channelName, int maximumBufferingPeriodInSecond) throws StorageModuleException {
        return legacyStorageProvider.createSession(channelName, maximumBufferingPeriodInSecond);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long renameSession(URI channelName, long sessionId) throws StorageModuleException {
        return legacyStorageProvider.renameSession(channelName, sessionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<Collection<Long>> getSessions(URI channelName) {
        return legacyStorageProvider.getSessions(channelName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<Collection<Long>> getSessions(long querySessionId) {
        return legacyStorageProvider.getSessions(querySessionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Set<Collection<Long>> getSessionsWithActiveMessages(long querySessionId) {
        return legacyStorageProvider.getSessionsWithActiveMessages(querySessionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void deleteMessagesByRange(long sessionId, long startMessageSeqNum, long endMessageSeqNum) throws StorageModuleException {
        legacyStorageProvider.deleteMessagesByRange(sessionId, startMessageSeqNum, endMessageSeqNum);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void deleteSession(long sessionId) throws StorageModuleException {
        legacyStorageProvider.deleteSession(sessionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long saveMessage(long sessionId, PersistentMessage persistentMessage) throws StorageModuleException {
        return legacyStorageProvider.saveMessage(sessionId, persistentMessage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized List<PersistentMessage> getMessages(long sessionId) throws StorageModuleException {
        return legacyStorageProvider.getMessages(sessionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized List<PersistentMessage> getMessages(long sessionId, int limit, int offset) throws StorageModuleException {
        return legacyStorageProvider.getMessages(sessionId, limit, offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean deleteMessages(long sessionId) throws StorageModuleException {
        return legacyStorageProvider.deleteMessages(sessionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void deleteMessage(long sessionId, long channelSequenceNumber) throws StorageModuleException {
        legacyStorageProvider.deleteMessage(sessionId, channelSequenceNumber);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized int getSessionSize(long sessionId) {
        return legacyStorageProvider.getSessionSize(sessionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long getPreviousSession(long sessionId) {
        return legacyStorageProvider.getPreviousSession(sessionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLatestActiveSession(URI channelName) throws StorageModuleException {
        return legacyStorageProvider.getLatestActiveSession(channelName);
    }
}