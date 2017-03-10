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

/**
 * Implements stopping a session recording. messages associated with the session
 * are not deleted. The JSON representation of this message would look like:<br>
 * <br>
 * <code>
 * {<br>
 * "@cmd" : "STOP",<br>
 * "sessionId" : 1234<br>
 * }<br>
 * </code>
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
class StopMessage extends BaseMessage {

    private final Long sessionId;

    /**
     * Constructor
     *
     * @param sessionId session ID to stop recording against
     */
    @JsonCreator
    StopMessage(@JsonProperty(value = "sessionId", required = true) Long sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * Stops the session ID from recording any future published messages on the
     * channel.
     *
     * @param context Storage module processing this message
     * @return boolean
     */
    @Override
    public Object onCommandMessage(StorageModule context) throws StorageModuleException {
        context.stopSessionChannel(sessionId);
        return true;
    }

    /**
     * )
     * get the session ID to stop recording for
     *
     * @return the session ID
     */
    public Long getSessionId() {
        return sessionId;
    }
}
