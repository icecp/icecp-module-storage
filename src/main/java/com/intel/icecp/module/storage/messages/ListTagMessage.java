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
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.exceptions.TaggingOperationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implements the list tag command for retrieving a list of tags under the specified channel URI.
 * <p>
 * The JSON representation of this command message would look like:<br>
 * <br>
 * <code>
 * {<br>
 * "@cmd" : "LIST_TAG",<br>
 * "query" : "ndn:/tag-channel/list_tag",<br>
 * }<br>
 * </code>
 *
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
public class ListTagMessage extends BaseMessage {
    private static final Logger LOGGER = LogManager.getLogger();
    private final String query;

    /**
     * Constructor with {@code query} argument.
     *
     * @param query a channel to reply the list of retrieved tags
     */
    @JsonCreator
    ListTagMessage(@JsonProperty(value = "query", required = true) String query) {
        this.query = query;
    }

    /**
     * Getter for tag channel.
     *
     * @return the string representation of tag channel to reply
     */
    public String getQuery() {
        return query;
    }

    @Override
    public Object onCommandMessage(StorageModule storageModule) throws StorageModuleException {
        LOGGER.debug("ListTagMessage received = " + this);

        validInputCheck();

        try {
            Set<Tag> tags = storageModule.getStorageProvider().related(new Tag(query));

            return tags.stream().map(Tag::toString).collect(Collectors.toSet());
        } catch (TaggingOperationException e) {
            throw new StorageModuleException(
                    String.format("Failed to list tags for query = %s. cause exception: %s", query, e));
        }
    }

    @Override
    public String toString() {
        return "ListTagMessage{query='" + query + "'} " + super.toString();
    }

    private void validInputCheck() throws StorageModuleException {
        if (Strings.isEmpty(query)) {
            LOGGER.error("channel URI string is empty!");
            throw new StorageModuleException(String.format("Invalid tag replay channel: %s", query));
        }
        // validate whether this channel is a well-formed URI or not
        try {
            new URI(query).toString();
        } catch (URISyntaxException e) {
            LOGGER.error("Invalid channel URI string {}!", e);
            throw new StorageModuleException(String.format("Invalid tag replay channel: %s", query));
        }
    }
}
