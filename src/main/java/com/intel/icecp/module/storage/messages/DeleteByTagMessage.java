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