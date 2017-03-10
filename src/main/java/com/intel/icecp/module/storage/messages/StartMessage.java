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
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.core.misc.ChannelIOException;
import com.intel.icecp.core.misc.ChannelLifetimeException;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.providers.StorageProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Implements the creation of a new session that will store messages from a channel. The JSON representation of this message would
 * look like:<br><br>
 * <code>
 * {<br>
 * "@cmd" : "START",<br>
 * "listenChannel" : "http://192.168.0.1/channel_to_listen<br>
 * }<br>
 * </code>
 *
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
class StartMessage extends BaseMessage {
    private static final long serialVersionUID = -7454923523871297832L;
    private static final Logger LOGGER = LogManager.getLogger(StartMessage.class.getName());

    private final String listenChannel;
    private final int maxBufferingPeriodInSec;

    /**
     * Constructor with listenChannel.  The maximum buffering period in this case is considered as
     * the default.
     *
     * @param listenChannel Channel to listen to and store data on
     */
    @JsonCreator
    StartMessage(@JsonProperty(value = "listenChannel", required = true) String listenChannel) {
        this(listenChannel, StorageModule.DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC);
    }

    /**
     * Constructor with both listenChannel and an integer maximum buffering period in second: if it is not positive,
     * then it will be default to {@code DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC}.
     *
     * @param listenChannel Channel to listen to and store data on
     * @param maxBufferingPeriodInSec maximum buffering period in second for data to be retained before it is uploaded
     */
    @JsonCreator
    StartMessage(@JsonProperty(value = "listenChannel", required = true) String listenChannel,
                 @JsonProperty(value = "maxBufferingPeriodInSec", required = true) int maxBufferingPeriodInSec) {
        this.listenChannel = listenChannel;
        this.maxBufferingPeriodInSec = (maxBufferingPeriodInSec > 0) ? maxBufferingPeriodInSec
                : StorageModule.DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC;
    }

    /**
     * Get the channel to listen on
     *
     * @return the channel to listen on
     */
    public String getListenChannel() {
        return listenChannel;
    }

    /**
     * Get the maximum buffering period in second for data retention before it is uploaded
     *
     * @return the positive integer number for buffering period in second of data storage retention
     */
    public int getMaxBufferingPeriodInSec() {
        return maxBufferingPeriodInSec;
    }

    /**
     * Subscribes to the requested channel.
     *
     * @param context Storage module processing this message
     * @return sessionId
     */
    @Override
    public Object onCommandMessage(StorageModule context) throws StorageModuleException {
        LOGGER.debug("StartMessage received = " + this);

        if (listenChannel == null) {
            throw new StorageModuleException("ListenChannel is null");
        }

        StorageProvider provider = context.getStorageProvider();
        Long sessionId = null;

        try {
            URI channelURI = new URI(this.listenChannel);
            provider.beginTransaction();
            Long previousSessionId = provider.getLatestActiveSession(channelURI);

            if (previousSessionId == 0) {
                sessionId = provider.createSession(channelURI, maxBufferingPeriodInSec);
            } else {
                sessionId = provider.renameSession(channelURI, previousSessionId);
            }
            provider.commitTransaction();
            LOGGER.debug("sessionId = {} created ", sessionId);

            Channel<BytesMessage> persistChannel = context.getNode().openChannel(channelURI, BytesMessage.class,
                    StorageModule.DEFAULT_PERSISTENCE);

            PersistCallback subscriptionCallback = new PersistCallback(context.getNode(), context.getStorageProvider(),
                    context.getAckChannel(), channelURI, sessionId);
            persistChannel.subscribe(subscriptionCallback);

            context.addChannel(sessionId, persistChannel, maxBufferingPeriodInSec, subscriptionCallback);
        } catch (StorageModuleException e) {
            provider.rollbackTransaction();
            throw e;
        } catch (ChannelLifetimeException e) {
            cleanupSession(provider, sessionId);
            throw new StorageModuleException(String.format("Channel %s failed to open. Error: %s", this.getListenChannel(), e));
        } catch (ChannelIOException e) {
            context.removeChannel(sessionId);
            cleanupSession(provider, sessionId);
            throw new StorageModuleException(String.format("Channel %s failed to subscribe. Error: %s", this.getListenChannel(), e));
        } catch (URISyntaxException e) {
            //in this case, session was never created (because the URI for the channel is bad, so no need to clean up
            throw new StorageModuleException(String.format("Channel %s has invalid URI format. Error: %s", this.getListenChannel(), e));
        }

        return sessionId;
    }


    private void cleanupSession(StorageProvider provider, Long sessionId) {
        try {
            provider.deleteSession(sessionId);
        } catch (StorageModuleException e) {
            LOGGER.error("Error during session cleanup of session ID: {}", sessionId, e);
        }
    }

    @Override
    public String toString() {
        return "StorageStartMessage{" + "listenChannel='" + listenChannel + '\'' + ", maxBufferingPeriodInSec="
                + maxBufferingPeriodInSec + "} " + super.toString();
    }
}
