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
