/*
 * ******************************************************************************
 *
 * INTEL CONFIDENTIAL
 *
 * Copyright 2013 - 2016 Intel Corporation All Rights Reserved.
 *
 * The source code contained or described herein and all documents related to
 * the source code ("Material") are owned by Intel Corporation or its suppliers
 * or licensors. Title to the Material remains with Intel Corporation or its
 * suppliers and licensors. The Material contains trade secrets and proprietary
 * and confidential information of Intel or its suppliers and licensors. The
 * Material is protected by worldwide copyright and trade secret laws and treaty
 * provisions. No part of the Material may be used, copied, reproduced,
 * modified, published, uploaded, posted, transmitted, distributed, or disclosed
 * in any way without Intel's prior express written permission.
 *
 * No license under any patent, copyright, trade secret or other intellectual
 * property right is granted to or conferred upon you by disclosure or delivery
 * of the Materials, either expressly, by implication, inducement, estoppel or
 * otherwise. Any license under such intellectual property rights must be
 * express and approved by Intel in writing.
 *
 * Unless otherwise agreed by Intel in writing, you may not remove or alter this
 * notice or any other notice embedded in Materials by Intel or Intel's
 * suppliers or licensors in any way.
 *
 * ******************************************************************************
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