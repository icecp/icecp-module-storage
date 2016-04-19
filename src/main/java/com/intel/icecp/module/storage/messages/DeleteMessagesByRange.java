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
