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
