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

/**
 * Implements the command message for deleting a range of messages within a
 * batch. The JSON representation of this message would look like:<br>
 * <br>
 * <code>
 * {<br>
 * "@cmd" : "DELETE_MESSAGE_BY_RANGE",<br>
 * "startMessageId" : 1,<br>
 * "endMessageId" : 7<br>
 * }
 * </code>
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
class DeleteMessagesByRange extends BaseMessage {
    private static final Logger LOGGER = LogManager.getLogger(DeleteSession.class.getName());

    private final Long sessionId;
    private final Integer startMessageSeqNum;
    private final Integer endMessageSeqNum;

    /**
     * Constructor
     *
     * @param sessionId sessionId of the messages.
     * @param startMessageSeqNum beginning Session ID to delete.
     * @param endMessageSeqNum ending Session ID to delete.
     */
    @JsonCreator
    DeleteMessagesByRange(@JsonProperty(value = "sessionId", required = true) Long sessionId,
                          @JsonProperty(value = "startMessageSeqNum", required = true) Integer startMessageSeqNum,
                          @JsonProperty(value = "endMessageSeqNum", required = true) Integer endMessageSeqNum) {
        this.setCmd(MessageType.DELETE_MESSAGE_BY_RANGE);
        this.sessionId = sessionId;
        this.startMessageSeqNum = startMessageSeqNum;
        this.endMessageSeqNum = endMessageSeqNum;
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
            context.getStorageProvider().beginTransaction();
            context.getStorageProvider().deleteMessagesByRange(sessionId, startMessageSeqNum, endMessageSeqNum);
            context.getStorageProvider().commitTransaction();
        } catch (StorageModuleException e) {
            context.getStorageProvider().rollbackTransaction();
            throw e;
        }

        return true;
    }

    @Override
    public String toString() {
        return "StorageDeleteMessagesByRange{sessionId=" + sessionId + ", start message sequence number="
                + startMessageSeqNum + ", end message sequence number=" + endMessageSeqNum + "} " + super.toString();
    }
}
