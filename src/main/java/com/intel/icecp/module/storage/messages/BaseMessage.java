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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.intel.icecp.core.Message;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.rpc.OnCommandMessage;

/**
 * Base class for all command messages sent to the Storage module
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@cmd", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = GetTimeSpan.class, name = "GET_TIME_SPAN"),
        @JsonSubTypes.Type(value = ListTagMessage.class, name = "LIST_TAG"),
        @JsonSubTypes.Type(value = DeleteSession.class, name = "DELETE_SESSION"),
        @JsonSubTypes.Type(value = DeleteMessagesByRange.class, name = "DELETE_MESSAGE_BY_RANGE"),
        @JsonSubTypes.Type(value = StartMessage.class, name = "START"),
        @JsonSubTypes.Type(value = GetMessage.class, name = "GET"),
        @JsonSubTypes.Type(value = StopMessage.class, name = "STOP"),
        @JsonSubTypes.Type(value = QueryMessage.class, name = "QUERY"),
        @JsonSubTypes.Type(value = RenameMessage.class, name = "RENAME"),
        @JsonSubTypes.Type(value = SizeMessage.class, name = "SIZE"),
        @JsonSubTypes.Type(value = DeleteByTagMessage.class, name = "DELETE_BY_TAG")})
@JsonInclude(value = JsonInclude.Include.NON_NULL)
public abstract class BaseMessage implements Message, OnCommandMessage<StorageModule, Object> {

    /**
     * Channel used to send command messages to the storage module.
     */
    // TODO: STORAGE_COMMAND_CHANNEL: Move this back to "Storage-CMD".
    public static final String COMMAND_CHANNEL_NAME = "ndn:/intel/storage/command";

    /**
     * Channel name to publish the response on. If null or empty, no response
     * will be set
     */
    private MessageType cmd;

    /**
     * Get the command message type
     *
     * @return the command message type
     */
    @JsonProperty("@cmd")
    public MessageType getCmd() {
        return cmd;
    }

    /**
     * Set the command message type
     *
     * @param cmd The message command type
     */
    public void setCmd(MessageType cmd) {
        this.cmd = cmd;
    }

    @Override
    public String toString() {
        return "BaseMessage{" + "cmd=" + getCmd() + '}';
    }

}
