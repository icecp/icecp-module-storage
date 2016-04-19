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

import com.intel.icecp.core.Channel;
import com.intel.icecp.core.Node;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.core.misc.ChannelIOException;
import com.intel.icecp.core.misc.OnPublish;
import com.intel.icecp.module.query.Queries;
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.ack.AckMessage;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.exceptions.TaggingOperationException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Class used to subscribe and store messages off a specific channel
 */
public class PersistCallback implements OnPublish<BytesMessage> {
    private static final Logger LOGGER = LogManager.getLogger(PersistCallback.class.getName());
    private final Node node;
    private final StorageProvider provider;
    private final Channel<AckMessage> ackMessageChannel;
    private final URI listenChannelUri;
    private long sessionId;
    private MessageDigest digest;

    /**
     * Constructor
     *
     * @param node the instance of the node the module is running on
     * @param provider the storage provider instance of this module
     * @param ackMessageChannel the acknowledgment channel
     * @param listenChannelUri the URI of the incoming channel of the received message
     * @param sessionId Session ID associated with this channel subscriber
     */
    public PersistCallback(Node node, StorageProvider provider, Channel<AckMessage> ackMessageChannel, URI listenChannelUri, Long sessionId) {
        this.node = node;
        this.provider = provider;
        this.ackMessageChannel = ackMessageChannel;
        this.listenChannelUri = listenChannelUri;
        this.sessionId = sessionId;

        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("Unable to create SHA-256 message digest", e);
            throw new IllegalStateException("Unable to create SHA-256 message digest; cannot proceed without this functionality", e);
        }
    }

    private static long toAcknowledgmentId(byte[] persistentMessageId) {
        return Arrays.hashCode(persistentMessageId);
    }

    /**
     * get the digest being used to create storage IDs
     *
     * @return the message digest object
     */
    MessageDigest getDigest() {
        return digest;
    }

    /**
     * Called whenever a message is published on the subscribed channel
     *
     * @param message message received on the subscribed channel
     */
    @Override
    public synchronized void onPublish(BytesMessage message) {
        LOGGER.debug("Received message for storage = {}", message);

        byte[] acknowledgmentHash = hashMessageContent(message);
        long messageId = 0;
        AckMessage ackMessage;
        try {
            provider.beginTransaction();
            messageId = provider.saveMessage(sessionId, new PersistentMessage(System.currentTimeMillis(), message.getBytes()));
            provider.commitTransaction();

            provider.beginTransaction();
            provider.tag(Queries.fromId(messageId), new Tag(listenChannelUri.toString()));
            provider.tag(Queries.fromId(messageId), new Tag(String.valueOf(sessionId)));
            provider.commitTransaction();

            ackMessage = new AckMessage(listenChannelUri, toAcknowledgmentId(acknowledgmentHash));
            ackMessageChannel.publish(ackMessage);
        } catch (StorageModuleException e) {
            provider.rollbackTransaction();
            LOGGER.error("Failed to save message '{}' with channel and session.", messageId, e);
        } catch (TaggingOperationException e) {
            LOGGER.error("Failed to tag messageId '{}' with channel and session.", messageId, e);
        } catch (ChannelIOException e) {
            LOGGER.error("Message save failed with storage id: {}", new String(acknowledgmentHash), e);
        }
    }

    private byte[] hashMessageContent(BytesMessage message) {
        digest.update(message.getBytes());
        byte[] returnBytes = digest.digest();
        digest.reset();
        return returnBytes;
    }

    /**
     * get the session ID for this subscription
     *
     * @return the session ID
     */
    public Long getSessionId() {
        return sessionId;
    }

    /**
     * update the sessionId for this subscription
     *
     * @param sessionId the sessionId
     */
    public void setSessionId(Long sessionId) {
        this.sessionId = sessionId;
    }

}
