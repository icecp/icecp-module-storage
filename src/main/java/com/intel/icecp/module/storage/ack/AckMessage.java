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
