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

package com.intel.icecp.module.storage.persistence;

import com.intel.icecp.core.Message;

/**
 * Message class which wraps a message to send over the channel to other modules. This represents both the data (i.e.
 * bytes) and metadata (i.e. assigned ID and timestamp) that are stored in the database.
 *
 */
public class PersistentMessage implements Message {
    private long id;
    private long timestamp;
    private byte[] content;


    public PersistentMessage() {
        // Empty constructor for Jackson
    }

    public PersistentMessage(long timestamp, byte[] messageContent) {
        this.id = -1;
        this.timestamp = timestamp;
        this.content = messageContent;
    }

    /**
     * Constructor with various arguments
     *
     * @param id the internal monotonic increasing integer per channel
     * @param timestamp the timestamp of the persistent message
     * @param messageContent the contents of the message
     */
    public PersistentMessage(long id, long timestamp, byte[] messageContent) {
        this.id = id;
        this.timestamp = timestamp;
        this.content = messageContent;
    }

    /**
     * @return the unique ID of the persisted message; IDs are ordered according to when they are persisted
     */
    public long getId() {
        return id;
    }

    /**
     * Used only for compatibility with the legacy storage provider; TODO remove when possible
     *
     * @param id the ID for the message, once saved
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * @return the Unix ms timestamp when the message was persisted
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return the bytes of the saved message
     */
    public byte[] getMessageContent() {
        return content;
    }

    @Override
    public String toString() {
        return "PersistentMessage{" + "id=" + id + ", timestamp=" + timestamp + ", content (size in bytes)=" + content.length + '}';
    }
}
