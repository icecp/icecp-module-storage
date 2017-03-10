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

package com.intel.icecp.module.storage.util;

import com.intel.icecp.core.Channel;
import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.messages.PersistCallback;

/**
 * Class for managing sessionIds in the Channels Map and Subscription Callback Map
 */
public class SessionIdManager {
    private final long sessionId;
    private StorageModule context;

    /**
     * Constructor
     *
     * @param context Storage module processing this message
     * @param sessionId Current active sessionId
     */
    public SessionIdManager(StorageModule context, Long sessionId) {
        this.context = context;
        this.sessionId = sessionId;
    }

    /**
     * Public method to update channel and callback references for renamed sessionIds during
     * rename and delete session
     *
     * @param newSessionId New sessionId obtained during rename/delete
     * @param channel Active listening channel for sessionId
     * @param callback Active callback registered for session
     */
    public synchronized void updateNewSessionId(Long newSessionId, Channel<BytesMessage> channel, PersistCallback callback) {
        callback.setSessionId(newSessionId);
        context.addChannel(newSessionId, channel, callback);
    }

    /**
     * Public method to cleanup channel and callback references for non-renamed sessionIds during
     * delete session
     */
    public synchronized void cleanupSessionId() {
        context.removeChannel(sessionId);
        context.removeSubscriptionCallback(sessionId);
    }

}
