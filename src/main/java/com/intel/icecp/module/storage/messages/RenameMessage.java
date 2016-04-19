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
