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
