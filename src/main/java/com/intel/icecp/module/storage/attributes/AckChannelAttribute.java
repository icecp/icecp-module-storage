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

package com.intel.icecp.module.storage.attributes;

import com.intel.icecp.core.attributes.BaseAttribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Used for defining the URI of the channel on which acknowledgments for persisted messages are sent on.
 * This attribute is defined in {@code configuration/config.json} like: <br>
 * <code>{"ack-channel" : "uri-of-the-ack-channel"}</code>
 *
 */
public class AckChannelAttribute extends BaseAttribute<URI> {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final String ACK_CHANNEL = "ack-channel";

    private final URI uri;

    /**
     * Constructor
     *
     * @param uri the (string) URI value of the attribute
     */
    public AckChannelAttribute(String uri) {
        super(AckChannelAttribute.ACK_CHANNEL, URI.class);

        if (uri == null) {
            this.uri = null;
            return;
        }

        URI readUri;
        try {
            readUri = new URI(uri);
        } catch (URISyntaxException e) {
            LOGGER.warn("Ack channel attribute received invalid URI <" + uri + ">", e);
            readUri = null;
        }
        assert readUri != null;

        this.uri = readUri;
    }

    public AckChannelAttribute(URI uri) {
        super(ACK_CHANNEL, URI.class);
        this.uri = uri;
    }

    @Override
    public URI value() {
        try {
            return new URI(uri.toString());
        } catch (URISyntaxException e) {
            LOGGER.error("Failed to clone the URI for external use, returning null.", e);
            return null;
        }
    }
}
