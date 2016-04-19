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
