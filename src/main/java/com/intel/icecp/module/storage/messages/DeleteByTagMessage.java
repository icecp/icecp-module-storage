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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.intel.icecp.module.query.Before;
import com.intel.icecp.module.query.Query;
import com.intel.icecp.module.query.Query.Identifier;
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.exceptions.TaggingOperationException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implements the command message for deleting a tagged messages. The JSON
 * representation of this message would look like:<br>
 * <br>
 * <code>
 * {<br>
 * "@cmd" : "DELETE_BY_TAGS",<br>
 * "tags" : ["tag1", "tag2"],<br>
 * "timestamp" : 1234L
 * <br> }
 * </code>
 *
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
class DeleteByTagMessage extends BaseMessage {
    private static final Logger LOGGER = LogManager.getLogger(DeleteSession.class.getName());

    private final String[] tags;
    private final Long before;

    /**
     * Constructor
     *
     * @param tags tags of messages to be removed.
     * @param before time before the current time to remove messages .
     */
    @JsonCreator
    DeleteByTagMessage(@JsonProperty(value = "tags", required = true) String[] tags,
                       @JsonProperty(value = "before", required = true) Long before) {
        this.setCmd(MessageType.DELETE_BY_TAG);
        this.tags = tags;
        this.before = before;
    }

    /**
     * Implements the deletion of tagged messages.
     *
     * @param context Storage module processing this message
     * @return size of the Set of deleted messages.
     */
    @Override
    public Object onCommandMessage(StorageModule context) throws StorageModuleException {
        LOGGER.debug("Message received = {}", this);
        Set<Identifier<?>> identifiers;

        // Null and Empty check for incoming Tags and Timestamp before creating
        // a query
        if (tags != null && tags.length != 0 && before != null) {
            identifiers = Arrays.stream(tags).map(Tag::new).collect(Collectors.toSet());
            identifiers.add(new Before(before));
        } else {
            throw new StorageModuleException("Invalid Tag Input");
        }

        Query query = new Query(identifiers.stream().toArray(Query.Identifier[]::new));

        try {
            context.getStorageProvider().beginTransaction();
            Set<PersistentMessage> deletedMessages = context.getStorageProvider().remove(query);
            context.getStorageProvider().commitTransaction();
            return deletedMessages.size();
        } catch (TaggingOperationException e) {
            context.getStorageProvider().rollbackTransaction();
            throw new StorageModuleException(String.format("Failed to delete messages with tags = %s. Exception: %s ",
                    Arrays.toString(tags), e));
        }

    }

    @Override
    public String toString() {
        return "DeleteByTag [tags=" + Arrays.toString(tags) + "]";
    }
}