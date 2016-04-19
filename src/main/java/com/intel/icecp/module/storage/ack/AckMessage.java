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

package com.intel.icecp.module.storage.ack;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.intel.icecp.core.Message;

import java.net.URI;

/**
 * Acknowledgment message structure which gets sent for every message persisted in the storage-module <br>
 * <p>
 * <br>
 * <code>
 * {
 * "uri" : "/uri-of-channel-the-incoming-message-was-received-on", <br>
 * "id" : "uniqueMessageId"<br>
 * }
 * </code>
 *
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "uri",
        "id"
})
public class AckMessage implements Message {
    private final URI uri;
    private final long id;

    /**
     * Constructor
     *
     * @param uri the URI of the channel a message was received on
     * @param id the message id
     */
    @JsonCreator
    public AckMessage(@JsonProperty(value = "uri", required = true) URI uri,
                      @JsonProperty(value = "id", required = true) long id) {
        this.uri = uri;
        this.id = id;
    }

    /**
     * Get the URI
     *
     * @return the URI of the channel a message was received on
     */
    public URI getUri() {
        return uri;
    }

    /**
     * Get the Id
     *
     * @return the message Id
     */
    public long getId() {
        return id;
    }
}
