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
import com.intel.icecp.module.storage.exceptions.InconsistentStateException;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.util.SessionIdManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Implements the command message for deleting all message in a session. The JSON representation of this message would look like:<br><br>
 * <code>
 * {<br>
 * "@cmd" : "DELETE_SESSION",<br>
 * "sessionId" : 1234<br>
 * }
 * </code>
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
class DeleteSession extends BaseMessage {
    private static final Logger LOGGER = LogManager.getLogger(DeleteSession.class.getName());

    private final Long sessionId;

    /**
     * Constructor
     *
     * @param sessionId Session ID to delete
     */
    @JsonCreator
    DeleteSession(@JsonProperty(value = "sessionId", required = true) Long sessionId) {
        this.setCmd(MessageType.DELETE_SESSION);
        this.sessionId = sessionId;
    }

    /**
     * Implements the deletion of a session.
     *
     * @param context Storage module processing this message
     * @return boolean true if deleted; otherwise, false.
     */
    @Override
    public Object onCommandMessage(StorageModule context) throws StorageModuleException {
        LOGGER.debug("Message received = " + this);
        if (sessionId == null) {
            throw new StorageModuleException("session ID is null");
        }

        try {
            Long attachedSessionId = context.getStorageProvider().getPreviousSession(sessionId);

            context.getStorageProvider().beginTransaction();
            context.getStorageProvider().deleteSession(sessionId);
            context.getStorageProvider().commitTransaction();

            // Check if sessionId is active or inactive
            if (isActiveSession(context)) {
                SessionIdManager sessionIdManager = new SessionIdManager(context, sessionId);

                checkIfRenamedSession(context, attachedSessionId, sessionIdManager);
                sessionIdManager.cleanupSessionId();
            }
            return true;
        } catch (StorageModuleException e) {
            context.getStorageProvider().rollbackTransaction();
            throw e;
        } catch (InconsistentStateException e) {
            throw new StorageModuleException(
                    String.format("Error while deleting session = %d. ", sessionId), e);
        }
    }

    private void checkIfRenamedSession(StorageModule context, Long attachedSessionId, SessionIdManager sessionIdManager) {
        PersistCallback callback = context.getCallback(sessionId);
        Optional<Channel<BytesMessage>> sessionChannel = context.getChannel(sessionId);

        // Check if its a renamed session
        if (attachedSessionId != 0L && callback != null && sessionChannel.isPresent()) {
            // Renamed active session
            sessionIdManager.updateNewSessionId(attachedSessionId, sessionChannel.get(), callback);
        }
    }

    /**
     * Checks if sessionId is active or inactive.
     *
     * @param context Storage module processing this message
     * @return true if active, else false
     * @throws InconsistentStateException if sessionId is active but is missing active channel/callback
     */
    private boolean isActiveSession(StorageModule context) throws InconsistentStateException {
        if (context.getCallback(sessionId) == null && !context.getChannel(sessionId).isPresent())
            return false;
        else if (context.getCallback(sessionId) == null || !context.getChannel(sessionId).isPresent())
            throw new InconsistentStateException(
                    String.format("Unable to find active channel/callback for session ID = %d", sessionId));

        return true;
    }

    @Override
    public String toString() {
        return "StorageDeleteSession{session ID=" + sessionId + "} " + super.toString();
    }
}
