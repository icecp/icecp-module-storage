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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.exceptions.TaggingOperationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements the query active timestamp range command to return active message timestamp range for messages currently
 * in the storage module under the specified data channel URI.
 * <p>
 * The JSON representation of this message would look like:<br>
 * <br>
 * <code>
 * {<br>
 * "@cmd" : "GET_TIME_SPAN",<br>
 * "queryChannel" : "ndn:/data-channel/b/c",<br>
 * }<br>
 * </code>
 *
 */
class GetTimeSpan extends BaseMessage {
    private static final Logger LOGGER = LogManager.getLogger();

    private final String queryChannel;

    /**
     * Constructor
     *
     * @param queryChannel channel containing active messages whose timestamp range is returned
     */
    @JsonCreator
    GetTimeSpan(@JsonProperty(value = "queryChannel", required = true) String queryChannel) {
        this.queryChannel = queryChannel;
    }

    /**
     * Get the channel to query on
     *
     * @return the channel to query on
     */
    public String getQueryChannel() {
        return queryChannel;
    }

    @Override
    public String toString() {
        return "StorageGetTimeSpanMessage{" + "queryChannel='" + queryChannel + "} " + super.toString();
    }

    /**
     * Implements the query active timestamp range command to return active message timestamp range for messages currently
     * in the storage module under the specified data channel URI.
     *
     * @param context Storage module processing this message
     * @return list of min and max timestamps
     * @throws StorageModuleException exception for storage module
     */
    @Override
    public Object onCommandMessage(StorageModule context) throws StorageModuleException {
        LOGGER.debug("Message received = " + this);

        try {
            URI.create(queryChannel);
        } catch (IllegalArgumentException e) {
            throw new StorageModuleException(String.format("Invalid query channel: %s. %s", queryChannel, e));
        }

        try {
            List<Long> timestampRange = new ArrayList<>();

            // add min/max timestamp to List, if min/max is same; send both entries
            timestampRange.add(context.getStorageProvider().getActiveMinimumTimestamp(queryChannel));
            timestampRange.add(context.getStorageProvider().getActiveMaximumTimestamp(queryChannel));
            return timestampRange;
        } catch (TaggingOperationException e) {
            throw new StorageModuleException(
                    String.format("Failed to get min/max time range for channel = %s. %s", queryChannel, e));
        }
    }

}