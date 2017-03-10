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

/**
 * Type of command messages supported by the storage module
 */
public enum MessageType {
    /**
     * Tag the identified messages with the provided tag name.
     */
    TAG,
    /**
     * Untag the identified messages containing the provided tag name.
     */
    UNTAG,
    /**
     * Deletes tagged messages
     */
    DELETE_BY_TAG,
    /**
     * Get the time range of active messages under a specified session channel
     */
    GET_TIME_SPAN,
    /**
     * Deletes all message in a storage session
     */
    DELETE_SESSION,
    /**
     * Deletes all messages in a specified range that exist in the same batch
     */
    DELETE_MESSAGE_BY_RANGE,
    /**
     * Starts a new session for storage
     */
    START,
    /**
     * Retrieves a set of messages for a storage session
     */
    GET,
    /**
     * Stops a storage session from recording
     */
    STOP,
    /**
     * Create a new session from an existing session
     */
    RENAME,
    /**
     * Find all sessions based on a channel
     */
    QUERY,
    /**
     * Retrieves number of messages in a session
     */
    SIZE
}
