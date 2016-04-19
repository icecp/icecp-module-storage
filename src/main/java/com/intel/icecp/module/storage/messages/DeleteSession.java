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
