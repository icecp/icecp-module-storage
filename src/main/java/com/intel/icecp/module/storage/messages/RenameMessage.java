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
import com.intel.icecp.core.Channel;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.util.SessionIdManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Implements the message for renaming a session (roughly equivalent to a stop
 * and start). The JSON representation of this message would look like:<br>
 * <br>
 * <code>
 * {<br>
 * "@cmd" : "RENAME",<br>
 * "sessionId" : 1234<br>
 * }<br>
 * </code>
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
class RenameMessage extends BaseMessage {
    private static final Logger LOGGER = LogManager.getLogger(RenameMessage.class.getName());

    private final Long sessionId;

    /**
     * Constructor
     *
     * @param sessionId sessionID to rename
     */
    @JsonCreator
    RenameMessage(@JsonProperty(value = "sessionId", required = true) Long sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * Implements the rename session functionality.
     *
     * @param context Storage module processing this message
     * @return Long newSessionId
     */
    @Override
    public Object onCommandMessage(StorageModule context) throws StorageModuleException {
        LOGGER.debug("Message received = " + this);
        if (sessionId == null) {
            throw new StorageModuleException("session ID is null");
        }

        PersistCallback callback = context.getCallback(sessionId);
        if (callback == null) {
            throw new StorageModuleException("subscription callback for session ID is null");
        }

        Optional<Channel<BytesMessage>> sessionChannelReference = context.getChannel(sessionId);

        if (sessionChannelReference.isPresent()) {
            Channel<BytesMessage> sessionChannel = sessionChannelReference.get();
            SessionIdManager sessionIdManager = new SessionIdManager(context, sessionId);

            try {
                context.getStorageProvider().beginTransaction();
                Long newSessionId = context.getStorageProvider().renameSession(sessionChannel.getName(), sessionId);
                context.getStorageProvider().commitTransaction();

                sessionIdManager.updateNewSessionId(newSessionId, sessionChannel, callback);
                sessionIdManager.cleanupSessionId();

                return newSessionId;
            } catch (StorageModuleException e) {
                context.getStorageProvider().rollbackTransaction();
                throw new StorageModuleException(
                        String.format("Error during renaming session = %d. %s", sessionId, e));
            }
        }

        throw new StorageModuleException(
                String.format("Unable to find channel for session ID = %d.", sessionId));
    }
}
