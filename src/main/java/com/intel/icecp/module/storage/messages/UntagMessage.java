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
import com.intel.icecp.module.query.Id;
import com.intel.icecp.module.query.Queries;
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.exceptions.TaggingOperationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Tags messages in the storage module with the tagName based
 * on the provided query.
 * <p>
 * The JSON representation of this message would look like:<br>
 * <br>
 * <code>
 * {<br>
 * "@cmd" : "UNTAG",<br>
 * "tags" : { ["abc, def, ghi"],
 * "ids" : [1, 2, 3] }<br>
 * }<br>
 * </code>
 *
 */
class UntagMessage extends BaseMessage {
    private static final Logger LOGGER = LogManager.getLogger();

    private final String[] tags;
    private final long[] ids;

    /**
     * Constructor
     *
     * @param tags tag name to be used in tagging messages.
     * @param ids identifiers of the message to be tagged.
     */
    @JsonCreator
    UntagMessage(String[] tags, long[] ids) {
        this.tags = tags;
        this.ids = ids;
    }

    /**
     * @param context Storage module processing this message
     * @return number of messageIds tagged.
     * @throws StorageModuleException exception for storage module
     */
    @Override
    public Object onCommandMessage(StorageModule context) throws StorageModuleException {
        LOGGER.debug("Message received = " + this);

        validateInput();

        Set<Long> vertexIdentifiers = new LinkedHashSet<>();

        context.getStorageProvider().beginTransaction();
        // TODO:  Add the Or query operator and use that instead.
        for (long id : ids) {
            for (String tagName : tags) {
                try {
                    Set<Id> taggedIds = context.getStorageProvider().untag(Queries.fromId(id), new Tag(tagName));
                    vertexIdentifiers.addAll(taggedIds.stream().map(Id::value).collect(Collectors.toList()));
                } catch (TaggingOperationException e) {
                    throw new StorageModuleException(String.format("Unable to untag '%s' on id '%d'.", tagName, id), e);
                }
            }
        }
        context.getStorageProvider().commitTransaction();

        return vertexIdentifiers.size();
    }

    private void validateInput() throws StorageModuleException {
        if (tags == null || tags.length == 0) {
            throw new StorageModuleException("Received an empty tagName set.");
        }

        if (ids == null || ids.length == 0) {
            throw new StorageModuleException("Received an empty id set.");
        }
    }

    @Override
    public String toString() {
        return "UntagMessage{TagName=" + Arrays.toString(tags) + "Id=" + Arrays.toString(ids) + "} " + super.toString();
    }
}
