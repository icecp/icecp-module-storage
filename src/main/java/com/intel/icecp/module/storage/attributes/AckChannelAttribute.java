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
