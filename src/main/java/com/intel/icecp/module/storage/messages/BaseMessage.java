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
