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

package com.intel.icecp.module.storage.persistence.providers;

import com.intel.icecp.module.storage.exceptions.TaggingOperationException;

/**
 * The interface for custom queries on the Storage Provider
 *
 */
public interface QueriesStorageProvider {

    /**
     * Get the minimum timestamp of all "active" messages on the given {@code channelName}
     *
     * @param channelName the name of the channel to search in
     * @return the smallest timestamp found on a message persisted on this channel
     * @throws TaggingOperationException if the operation fails
     */
    long getActiveMinimumTimestamp(String channelName) throws TaggingOperationException;

    /**
     * Get the maximum timestamp of all "active" messages on the given {@code channelName}
     *
     * @param channelName the name of the channel to search in
     * @return the largest timestamp found on a message persisted on this channel
     * @throws TaggingOperationException if the operation fails
     */
    long getActiveMaximumTimestamp(String channelName) throws TaggingOperationException;
}
