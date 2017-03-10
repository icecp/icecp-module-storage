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
