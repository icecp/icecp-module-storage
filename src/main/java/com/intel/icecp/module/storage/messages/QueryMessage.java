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

package com.intel.icecp.module.storage.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Set;

/**
 * Implements the query message for finding sessions on a channel. The JSON
 * representation of this message would look like:<br>
 * <br>
 * <code>
 * {<br>
 * "@cmd" : "QUERY",<br>
 * "queryChannel" : "http://192.168.0.1/datachannel",<br>
 * "sessionId" : 1234
 * }<br>
 * </code>
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
class QueryMessage extends BaseMessage {
    private static final Logger LOGGER = LogManager.getLogger(QueryMessage.class.getName());
    private final String queryChannel;
    private final Long querySessionId;
    private final boolean onlyWithActiveMessages;
    /**
     * Constructor
     *
     * @param queryChannel channel to search for in the sessions
     */
    @JsonCreator
    QueryMessage(@JsonProperty(value = "queryChannel") String queryChannel) {
        this.queryChannel = queryChannel;
        this.querySessionId = null;
        this.onlyWithActiveMessages = false;
    }

    /**
     * Constructor
     *
     * @param querySessionId sessionId to search for linked sessions
     */
    @JsonCreator
    QueryMessage(@JsonProperty(value = "sessionId") Long querySessionId) {
        this.querySessionId = querySessionId;
        this.queryChannel = null;
        this.onlyWithActiveMessages = false;
    }

    /**
     * Constructor
     *
     * @param querySessionId sessionId to search for linked sessions
     * @param onlyWithActiveMessages boolean to indicate those with active messages
     */
    @JsonCreator
    QueryMessage(@JsonProperty(value = "sessionId") Long querySessionId, @JsonProperty(value="onlyWithActiveMessages") boolean onlyWithActiveMessages) {
        this.querySessionId = querySessionId;
        this.queryChannel = null;
        this.onlyWithActiveMessages = onlyWithActiveMessages;
    }

    /**
     * Implements the channel query to return back sessions listening to this
     * channel.
     *
     * @param context Storage module processing this message
     * @return Set<Collection<Long>> sessionIds
     * @throws StorageModuleException exception for storage module
     */
    @Override
    public Object onCommandMessage(StorageModule context) throws StorageModuleException {
        LOGGER.debug("Message received = " + this);
        if (queryChannel == null && querySessionId == null) {
            throw new StorageModuleException("Command Error occurred in Query message.");
        }

        return queryChannel != null ? queryByChannelName(context) : queryBySessionId(context, onlyWithActiveMessages);
    }

    private Object queryByChannelName(StorageModule context) throws StorageModuleException {
        try {
            context.getStorageProvider().beginTransaction();
            Set<Collection<Long>> sessionIdCollection = context.getStorageProvider()
                    .getSessions(new URI(queryChannel));
            context.getStorageProvider().commitTransaction();
            return sessionIdCollection;
        } catch (URISyntaxException e) {
            throw new StorageModuleException(String.format(
                    "Invalid queryChannel URI syntax, queryChannel = %s. %s", queryChannel, e));
        }
    }

    private Object queryBySessionId(StorageModule context, boolean onlyWithActiveMessages) throws StorageModuleException {
        context.getStorageProvider().beginTransaction();
        Set<Collection<Long>> sessionIdCollection =  onlyWithActiveMessages ?
                context.getStorageProvider().getSessionsWithActiveMessages(querySessionId) :
                context.getStorageProvider().getSessions(querySessionId);
        context.getStorageProvider().commitTransaction();

        if (sessionIdCollection.isEmpty()) {
            throw new StorageModuleException(String.format("Invalid querySessionId = %s", querySessionId));
        }

        return sessionIdCollection;
    }

    @Override
    public String toString() {
        return "StorageQueryMessage{" + "queryChannel='" + queryChannel + "\'" + "querySessionId=" + querySessionId
                + "} " + super.toString();
    }
}
