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
