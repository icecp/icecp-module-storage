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
import com.intel.icecp.core.Channel;
import com.intel.icecp.core.misc.ChannelIOException;
import com.intel.icecp.core.misc.ChannelLifetimeException;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Implements the command message for retrieving messages from a session. The
 * JSON representation of this message would look like:
 * <pre><code>
 * {
 * "@cmd" : "GET",
 * "sessionId" : 1234,
 * "limit" : 10,
 * "skip": 0,
 * "replayChannel" : "http://192.168.0.1/mychannel/1234",
 * "responseChannel" : "http://192.168.0.1/mychannel/1234/response"
 * }
 * </code></pre>
 * TODO if possible, this method should allow querying by tags to avoid the hardcoded behavior of the 'inactive' tag,
 * see LegacyOrientDbStorageProvider.
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
class GetMessage extends BaseMessage {
    private static final Logger LOGGER = LogManager.getLogger(GetMessage.class.getName());

    private final Long sessionId;
    private final String replayChannel;
    private Integer limit;
    private Integer skip;

    /**
     * @param sessionId Session ID to get messages from
     * @param limit (optional) limit for number of messages to get. If not supplied, all messages for this session will
     * be retrieved.
     * @param skip (optional) number of messages to skip before retrieving messages. If not supplied, retrieval will
     * start with the oldest (first) message
     * @param replayChannel Channel to publish the messages on
     */
    @JsonCreator
    GetMessage(@JsonProperty(value = "sessionId", required = true) Long sessionId,
               @JsonProperty("limit") Integer limit, @JsonProperty("skip") Integer skip,
               @JsonProperty(value = "replayChannel", required = true) String replayChannel) {
        this.setCmd(MessageType.GET);
        this.sessionId = sessionId;
        this.limit = limit;
        this.skip = skip;
        this.replayChannel = replayChannel;
    }

    /**
     * get the skip number
     *
     * @return The number of messages to skip
     */
    public Integer getSkip() {
        return skip;
    }

    /**
     * get the limit number
     *
     * @return The number of messages to return
     */
    public Integer getLimit() {
        return limit;
    }

    /**
     * Implements the retrieval of messages for a session
     *
     * @param context Storage module processing this message
     * @return int number of messages.
     */
    @Override
    public Object onCommandMessage(StorageModule context) throws StorageModuleException {
        int publishedMessageCount = 0;
        LOGGER.debug("Message received = " + this);

        validInputCheck();

        try (Channel<PersistentMessage> replay = context.getNode().openChannel(new URI(replayChannel), PersistentMessage.class,
                StorageModule.DEFAULT_PERSISTENCE)) {

            if (limit < 0 || skip < 0) {
                throw new StorageModuleException(String.format(
                        "Invalid values for limit = %d or offset = %d , channel = %s.  %d messages were published:",
                        limit, skip, replayChannel, publishedMessageCount));
            }

            List<PersistentMessage> messages = context.getStorageProvider().getMessages(sessionId, limit, skip);

            LOGGER.debug("Replaying {} messages", messages.size());

            for (PersistentMessage msg : messages) {
                replay.publish(msg);
                publishedMessageCount++;
            }

            return publishedMessageCount;
        } catch (ChannelLifetimeException | ChannelIOException e) {
            throw new StorageModuleException(
                    String.format("Replay channel exception, channel = %s.  %d messages were published: %s",
                            replayChannel, publishedMessageCount, e));
        } catch (URISyntaxException e) {
            throw new StorageModuleException(
                    String.format("Invalid URI for replay channel = %s.  %d messages were published: %s", replayChannel,
                            publishedMessageCount, e));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new StorageModuleException(String.format(
                    "Null values for limit = %d or offset = %d , channel = %s.  %d messages were published: %s", limit,
                    skip, replayChannel, publishedMessageCount, e));
        }
    }

    private void validInputCheck() throws StorageModuleException {
        if (sessionId == null) {
            throw new StorageModuleException("SessionId is null");
        }

        // TODO: Do we want to use a default channel if a replay channel is not
        // specified?
        if (Strings.isEmpty(replayChannel)) {
            throw new StorageModuleException(String.format("Invalid replay channel, channel = %s", replayChannel));
        }
    }

    @Override
    public String toString() {
        return "GetMessage{" + "sessionId=" + sessionId + ", limit=" + limit + ", skip=" + skip + ", replayChannel='"
                + replayChannel + '\'' + "} " + super.toString();
    }
}
