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

package com.intel.icecp.module.storage;

import com.intel.icecp.core.Channel;
import com.intel.icecp.core.Module;
import com.intel.icecp.core.Node;
import com.intel.icecp.core.attributes.AttributeNotFoundException;
import com.intel.icecp.core.attributes.AttributeNotWriteableException;
import com.intel.icecp.core.attributes.Attributes;
import com.intel.icecp.core.attributes.IdAttribute;
import com.intel.icecp.core.attributes.ModuleStateAttribute;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.core.metadata.Persistence;
import com.intel.icecp.core.misc.ChannelIOException;
import com.intel.icecp.core.misc.ChannelLifetimeException;
import com.intel.icecp.core.misc.Configuration;
import com.intel.icecp.core.modules.ModuleProperty;
import com.intel.icecp.module.storage.ack.AckMessage;
import com.intel.icecp.module.storage.attributes.AckChannelAttribute;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.messages.BaseMessage;
import com.intel.icecp.module.storage.messages.CommandAdapter;
import com.intel.icecp.module.storage.messages.PersistCallback;
import com.intel.icecp.module.storage.persistence.orientdb.StorageProviderFacade;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import com.intel.icecp.rpc.Command;
import com.intel.icecp.rpc.Rpc;
import com.intel.icecp.rpc.RpcServer;
import org.apache.commons.lang.NullArgumentException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Module that implements persistent storage capabilities
 *
 */
@ModuleProperty(name = "StorageModule", attributes = {AckChannelAttribute.class})
public class StorageModule implements Module {
    public static final Persistence DEFAULT_PERSISTENCE = new Persistence(10000, 10000);
    // default buffering period in second is forever,  
    // represented by Integer.MAX_VALUE, before upload
    public static final int DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC = Integer.MAX_VALUE;
    private static final Logger LOGGER = LogManager.getLogger(StorageModule.class.getName());

    private final StorageProvider provider;
    private final ConcurrentHashMap<Long, Session> sessions;
    private Channel<AckMessage> ackMessageChannel;
    private Node node;
    private Attributes storageAttributes;
    private long moduleId;
    private boolean running;
    private RpcServer rpcServer;

    /**
     * Constructor
     */
    public StorageModule() {
        this(newDefaultProvider());
    }

    /**
     * Constructor with provider input
     *
     * @param provider Storage provider used for this module
     */
    public StorageModule(StorageProvider provider) {
        running = false;
        sessions = new ConcurrentHashMap<>();
        this.provider = (provider != null) ? provider : newDefaultProvider();
    }

    private static StorageProvider newDefaultProvider() {
        return new StorageProviderFacade();
    }

    public Channel<AckMessage> getAckChannel() {
        return ackMessageChannel; // TODO replace this with better solution: ackMessageChannel per incoming data channel
    }

    /**
     * get the node the module is running in
     *
     * @return the node
     */
    public Node getNode() {
        return node;
    }

    RpcServer getRpcServer() {
        return rpcServer;
    }

    /**
     * @deprecated use {@link #run(Node, Attributes)} instead.
     */
    @Override
    @Deprecated
    public void run(Node node, Configuration moduleConfiguration, Channel<State> moduleStateChannel, long moduleId) {
        throw new UnsupportedOperationException("Deprecated version of run, will be removed entirely in a future release");
    }

    /**
     * Storage module for persisting messages
     * <p>
     * Ex: This module will receive messages from icecp-module-fork which it will persist into it's database
     *
     * @param node the node the module is currently running on
     * @param attributes set of attributes {@link Attributes} defined for the module
     */
    @Override
    public void run(Node node, Attributes attributes) {
        this.node = node;
        this.storageAttributes = attributes;

        try {
            this.moduleId = storageAttributes.get(IdAttribute.class);
            URI uri = storageAttributes.get(AckChannelAttribute.class);
            if (uri != null) {
                this.ackMessageChannel = node.openChannel(uri, AckMessage.class, Persistence.DEFAULT);
            }

            // TODO: In the future, do this with ModuleStateAttribute
            if (running) {
                setAttribute(ModuleStateAttribute.class, State.RUNNING);
                LOGGER.warn("StorageModule run called again, node={}, attributes={}, id={}", this.node, this.storageAttributes,
                        this.moduleId);
                return;
            }

            if (node == null) {
                setAttribute(ModuleStateAttribute.class, State.ERROR);
                return;
            }

            createServer();
            running = true;
            setAttribute(ModuleStateAttribute.class, State.RUNNING);
            LOGGER.debug("StorageModule running, node={}, attributes={}, id={}", this.node, this.storageAttributes, this.moduleId);
        } catch (ChannelLifetimeException | ChannelIOException e) {
            LOGGER.error("Failed to open/perform IO on channel", e);
            setAttribute(ModuleStateAttribute.class, State.ERROR);
        } catch (AttributeNotFoundException e) {
            LOGGER.error("Attribute not found", e);
            setAttribute(ModuleStateAttribute.class, State.ERROR);
        }
    }

    /**
     * Set an attribute with class and value pair with error handling
     *
     * @param attributeClass class of the attribute to be set
     * @param attributeValue value of the attribute to be set
     */
    private void setAttribute(Class attributeClass, Object attributeValue) {
        try {
            storageAttributes.set(attributeClass, attributeValue);
        } catch (AttributeNotFoundException | AttributeNotWriteableException e) {
            LOGGER.error("Attribute {} could not be set", attributeClass.getName(), e);
        }
    }

    /**
     * Adds commands to the CommandRegistry and creates a new RpcServer instance
     * to create a listening channel.
     *
     * @throws ChannelIOException when RPC server fails to serve using channels
     * @throws ChannelLifetimeException when RPC server fails to open channels
     */
    private void createServer() throws ChannelIOException, ChannelLifetimeException {
        // TODO: STORAGE_COMMAND_CHANNEL: Move this back to a module based command channel when multiple storage modules are required.
        rpcServer = Rpc.newServer(node.channels(), URI.create(BaseMessage.COMMAND_CHANNEL_NAME));
        addCommands();
        rpcServer.registry().list().forEach(LOGGER::info);
        rpcServer.serve();
    }

    /**
     * Adds commands to the RpcServer command registry.
     */
    private void addCommands() {
        CommandAdapter adapter = new CommandAdapter(this);
        Arrays.asList(adapter.getClass().getDeclaredMethods()).stream()
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .map(m -> new Command(m.getName(), adapter, m))
                .forEach(rpcServer.registry()::add);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop(StopReason stopReason) {
        try {
            if (rpcServer != null) {
                closeAllStorageModuleChannels();
                // no shutdown of database provider as the current is using database pooling. If shutdown, it causes issues on restart.
                setAttribute(ModuleStateAttribute.class, State.STOPPED);
                rpcServer.close();
            }
        } catch (ChannelLifetimeException e) {
            LOGGER.error("Failed to close commandChannel", e);
            setAttribute(ModuleStateAttribute.class, State.ERROR);
        }
        LOGGER.debug("Stopping module id = {}, reason = {}", this.moduleId, stopReason);
    }

    /**
     * close all channels open by the storage module
     */
    private void closeAllStorageModuleChannels() {
        for (Entry<Long, Session> session : sessions.entrySet()) {
            try {
                stopSessionChannel(session.getKey());
            } catch (StorageModuleException e) {
                LOGGER.error("Failed to stop session {} Channel", session.getKey(), e);
            }
        }
    }

    /**
     * Add a channel reference to the list
     *
     * @param sessionId Session ID for the channel
     * @param channel Channel reference
     */
    public void addChannel(Long sessionId, Channel<BytesMessage> channel) {
        addChannel(sessionId, channel, DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC, null);
    }

    /**
     * Add a channel reference to the list with configurable maximum buffering
     * period.
     *
     * @param sessionId Session ID for the channel
     * @param channel Channel reference
     * @param maxBufferingPeriodInSec Maximum integral value for buffering period in second for data
     * kept in the storage module before it is uploaded successfully
     */
    public void addChannel(Long sessionId, Channel<BytesMessage> channel, Integer maxBufferingPeriodInSec) {
        addChannel(sessionId, channel, maxBufferingPeriodInSec, null);
    }

    /**
     * Add a channel reference to the list with persist callback.
     *
     * @param sessionId Session ID for the channel
     * @param channel Channel reference
     * @param callback Channel callback reference
     */
    public void addChannel(Long sessionId, Channel<BytesMessage> channel, PersistCallback callback) {
        addChannel(sessionId, channel, DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC, callback);
    }

    /**
     * Add a channel reference to the list with configurable maximum buffering
     * period.
     *
     * @param sessionId Session ID for the channel
     * @param channel Channel reference
     * @param maxBufferingPeriodInSec Maximum integral value for buffering period in second for data
     * kept in the storage module before it is uploaded successfully
     * @param callback Channel callback reference
     */
    public void addChannel(Long sessionId, Channel<BytesMessage> channel, Integer maxBufferingPeriodInSec, PersistCallback callback) {
        if (sessionId == null) {
            throw new NullArgumentException("sessionId");
        }

        if (channel == null) {
            throw new NullArgumentException("channel");
        }

        if (maxBufferingPeriodInSec == null) {
            throw new NullArgumentException("maxBufferingPeriodInSec");
        }

        Session session = sessions.get(sessionId);
        if (session != null) {
            session.channels = channel;
            session.maxBufferingPeriods = maxBufferingPeriodInSec;
            session.subscriptionCallbacks = callback;
        } else {
            session = new Session(channel, maxBufferingPeriodInSec, callback);
        }
        sessions.put(sessionId, session);
    }

    /**
     * Get the subscription callback reference for the sessionId
     *
     * @param sessionId Session for the subscription callback reference
     * @return the subscription callback reference for the session
     */
    public PersistCallback getCallback(Long sessionId) {
        Session session = sessions.get(sessionId);
        return session != null ? session.subscriptionCallbacks : null;
    }

    /**
     * Remove subscription callback for a sessionId
     *
     * @param sessionId sessionId to remove
     * @return true if channel was closed and removed successfully, false if
     * session was not found or channel close failed
     */
    public boolean removeSubscriptionCallback(Long sessionId) {
        PersistCallback callback = getCallback(sessionId);
        if (callback == null) {
            LOGGER.warn("Subscription callback does not exist for sessionId = {}", sessionId);
            return false;
        }

        if (sessions.get(sessionId) != null) {
            sessions.get(sessionId).subscriptionCallbacks = null;
        } else {
            LOGGER.warn("Session doesn't exist, sessionId = {}", sessionId);
            return false;
        }

        return true;
    }

    /**
     * Get the channel reference for the sessionId
     *
     * @param sessionId Session for the channel reference
     * @return the channel reference for the session
     */
    public Optional<Channel<BytesMessage>> getChannel(Long sessionId) {
        Session session = sessions.get(sessionId);
        return Optional.ofNullable(session != null ? session.channels : null);
    }

    /**
     * Get the maximum buffering period for the sessionId
     *
     * @param sessionId session identifier
     * @return the maximum buffering period in second as integer
     */
    public Optional<Integer> getMaximumBufferingPeriodInSecond(Long sessionId) {
        Session session = sessions.get(sessionId);
        return Optional.ofNullable(session != null ? session.maxBufferingPeriods : null);
    }

    /**
     * cleans up channels for a session
     *
     * @param sessionId sessionId to remove
     * @throws StorageModuleException when sessionId is null or unable to close channel.
     */
    public void stopSessionChannel(Long sessionId) throws StorageModuleException {
        LOGGER.debug("Stopping channel for session {}", sessionId);
        if (sessionId == null) {
            throw new StorageModuleException("Received a null sessionId");
        }

        if (!closeChannel(sessionId) || !removeChannel(sessionId) || !removeSubscriptionCallback(sessionId)) {
            throw new StorageModuleException(String.format("Unable to stop recording for session Id: %d", sessionId));
        }

        // If the session channel is closed then remove it from the sessions map
        sessions.remove(sessionId);
    }

    /**
     * Remove a channel for a session
     *
     * @param sessionId sessionId to remove
     * @return true if channel was removed successfully, false if
     * session was not found.
     */
    public boolean removeChannel(Long sessionId) {
        Optional<Channel<BytesMessage>> channel = getChannel(sessionId);
        if (!channel.isPresent()) {
            LOGGER.warn("Open channel does not exist for sessionId = {}", sessionId);
            return false;
        }

        if (sessions.get(sessionId) != null) {
            // Do not close channels over in this method, it closes the active listening channel
            sessions.get(sessionId).channels = null;
        } else {
            LOGGER.warn("Session doesn't exist, sessionId = {}", sessionId);
            return false;
        }

        return true;
    }

    /**
     * close a channel for a session
     *
     * @param sessionId sessionId to remove
     * @return true if channel was closed successfully, false if
     * session was not found.
     */
    public boolean closeChannel(Long sessionId) {
        Optional<Channel<BytesMessage>> channel = getChannel(sessionId);
        if (!channel.isPresent()) {
            LOGGER.warn("Open channel does not exist for sessionId = {}", sessionId);
            return false;
        }

        Session session = sessions.get(sessionId);
        if (session != null) {
            try {
                if (session.channels != null)
                    session.channels.close();
            } catch (ChannelLifetimeException e) {
                LOGGER.error("Unable to close channel", e);
                return false;
            }
        } else {
            LOGGER.warn("Session doesn't exist, sessionId = {}", sessionId);
            return false;
        }

        return true;
    }

    /**
     * Get all channels held by the module
     *
     * @return list of open channels
     */
    public synchronized Collection<Channel<BytesMessage>> getAllChannels() {
        Collection<Session> allSessions = sessions.values();
        Collection<Channel<BytesMessage>> channels = new ArrayList<>();
        for (Session session : allSessions) {
            if (session.channels != null) {
                channels.add(session.channels);
            }
        }
        return channels;
    }

    /**
     * Get all callbacks held by the module
     *
     * @return list of subscription callbacks
     */
    public synchronized Collection<PersistCallback> getAllCallbacks() {
        Collection<Session> allSessions = sessions.values();
        Collection<PersistCallback> callbacks = new ArrayList<>();
        for (Session session : allSessions) {
            if (session.subscriptionCallbacks != null) {
                callbacks.add(session.subscriptionCallbacks);
            }
        }
        return callbacks;
    }

    /**
     * Gets the storage provider to use for command message processing
     *
     * @return the storage provider
     */
    public StorageProvider getStorageProvider() {
        // TODO- type of storage provider should be based on the configuration
        return provider;
    }

    static class Session {
        private Channel<BytesMessage> channels;
        private Integer maxBufferingPeriods;
        private PersistCallback subscriptionCallbacks;

        Session(Channel<BytesMessage> channels, Integer maxBufferingPeriods) {
            this(channels, maxBufferingPeriods, null);
        }

        Session(Channel<BytesMessage> channels, Integer maxBufferingPeriods, PersistCallback subscriptionCallbacks) {
            this.channels = channels;
            this.maxBufferingPeriods = maxBufferingPeriods;
            this.subscriptionCallbacks = subscriptionCallbacks;
        }
    }
}
