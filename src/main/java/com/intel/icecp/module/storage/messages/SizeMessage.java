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
 * Implements the query to get the number of messages in a session. The JSON representation of this message would
 * look like:<br><br>
 * <code>
 * {<br>
 * "@cmd" : "SIZE",<br>
 * "sessionId" : 1234<br>
 * }<br>
 * </code>
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
class SizeMessage extends BaseMessage {
    private static final Logger LOGGER = LogManager.getLogger(SizeMessage.class.getName());

    private final Long sessionId;

    /**
     * Constructor
     *
     * @param sessionId session ID to query for number of messages
     */
    @JsonCreator
    public SizeMessage(@JsonProperty(value = "sessionId", required = true) Long sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * Implements the rename session functionality.
     *
     * @param context Storage module processing this message
     * @return {@link SessionSizeResponse} message.
     */
    @Override
    public Object onCommandMessage(StorageModule context) throws StorageModuleException {
        LOGGER.debug("Message received = " + this);
        if (getSessionId() == null) {
            throw new StorageModuleException("session ID is null");
        }

        return context.getStorageProvider().getSessionSize(sessionId);
    }

    /**
     * get the session ID to query for message count
     *
     * @return the session ID
     */
    public Long getSessionId() {
        return sessionId;
    }
}
