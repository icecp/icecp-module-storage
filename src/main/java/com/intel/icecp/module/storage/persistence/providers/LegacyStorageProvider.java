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

import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * The interface for the Legacy Storage provider.
 */
public interface LegacyStorageProvider {
    /**
     * Begin database transaction
     * @throws StorageModuleException 
     */
    void beginTransaction() throws StorageModuleException;

    /**
     * Commit database transaction
     */
    void commitTransaction();

    /**
     * Rollback database transaction
     */
    void rollbackTransaction();

    /**
     * Gets all channels for which there are sessions.
     *
     * @return the channels
     */
    Set<URI> getChannels();

    /**
     * Create session for a given channel.
     *
     * @param channelName the channel name
     * @return the session identifier
     * @throws StorageModuleException
     */
    long createSession(URI channelName) throws StorageModuleException;

    /**
     * Create session for a given channel with maximum buffering period in
     * second associated with the session.
     *
     * @param channelName the channel name
     * @param maximumBufferingPeriodInSecond the buffer period in second to hold to-be-uploaded data in the
     * storage
     * @return the new session identifier
     * @throws StorageModuleException
     */
    long createSession(URI channelName, int maximumBufferingPeriodInSecond) throws StorageModuleException;

    /**
     * Creates a new session linked to an older session. The older session is
     * closed.
     *
     * @param channelName the channel name
     * @param sessionId the session identifier to rename
     * @return the new session identifier created by rename
     * @throws StorageModuleException
     */
    long renameSession(URI channelName, long sessionId) throws StorageModuleException;

    /**
     * Gets linked sessions for a given channel URI. The newest session starts
     * the collection in the linked sessions.
     *
     * @param channelName the channel name URI
     * @return set of linked sessions
     */
    Set<Collection<Long>> getSessions(URI channelName);

    /**
     * Gets linked sessions for a given session Id. The newest session starts
     * the collection in the linked sessions.
     *
     * @param querySessionId the session identifier
     * @return set of linked sessions
     */
    Set<Collection<Long>> getSessions(long querySessionId);

    /**
     * Gets only those linked sessions with active messages for a given session Id. The newest session starts
     * the collection in the linked sessions.
     *
     * @param querySessionId the session identifier
     * @return set of linked sessions with active messages
     */
    Set<Collection<Long>> getSessionsWithActiveMessages(long querySessionId);

    /**
     * Delete a range of messages from a session.
     *
     * @param sessionId the session id to be deleted.
     * @param startMessageSeqNum starting message channel sequence number.
     * @param endMessageSeqNum ending message channel sequence number.
     * @throws StorageModuleException database state is not consistent
     */
    void deleteMessagesByRange(long sessionId, long startMessageSeqNum, long endMessageSeqNum)
            throws StorageModuleException;

    /**
     * Delete session. When the to-be-deleted session is the last session in
     * that channel, the underlying channel will also be deleted.
     *
     * @param sessionId the session id to be deleted
     * @throws StorageModuleException unable to delete session
     */
    void deleteSession(long sessionId) throws StorageModuleException;

    /**
     * Save message with a given {@code sessionId}.
     *
     * @param sessionId the session id
     * @param persistentMessage the persistent message
     * @return message Id
     * @throws StorageModuleException when unable to save message
     */
    long saveMessage(long sessionId, PersistentMessage persistentMessage) throws StorageModuleException;

    /**
     * Gets all messages for a given {@code sessionId}.
     *
     * @param sessionId the session id
     * @return a list of messages under that given {@code sessionId}
     * @throws StorageModuleException
     */
    List<PersistentMessage> getMessages(long sessionId) throws StorageModuleException;

    /**
     * Gets messages for a given {@code sessionId} with starting offset and the
     * limit number of messages.
     *
     * @param sessionId the session id
     * @param limit the maximum number of messages to return
     * @param offset the number of messages to skip before returning messages
     * @return a list of messages under the searching criteria
     * @throws StorageModuleException
     */
    List<PersistentMessage> getMessages(long sessionId, int limit, int offset) throws StorageModuleException;

    /**
     * Delete messages for a given {@code sessionId}.
     *
     * @param sessionId the session id
     * @return boolean to indicate success deletion
     * @throws StorageModuleException
     */
    boolean deleteMessages(long sessionId) throws StorageModuleException;

    /**
     * Delete messages for a given {@code sessionId} and
     * {@code persistentMessageId} in byte format.
     *
     * @param sessionId the session id
     * @param channelSequenceNumber the channel sequence number
     * @throws StorageModuleException
     */
    void deleteMessage(long sessionId, long channelSequenceNumber) throws StorageModuleException;

    /**
     * Gets the number of messages for a given {@code sessionId}.
     *
     * @param sessionId the session id
     * @return an non-negative integer number to indicate the number of messages
     * associated with a given session
     */
    int getSessionSize(long sessionId);

    /**
     * Get previous linked session.
     *
     * @param sessionId the session id
     * @return the previous linked sessionId
     */
    long getPreviousSession(long sessionId);

     /** Get the last active session found in the database.
     *
     * @param channelName the channel name
     * @return the session identifier
     * @throws StorageModuleException
     */
    long getLatestActiveSession(URI channelName) throws StorageModuleException;
}
