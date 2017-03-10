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

package com.intel.icecp.module.storage.persistence.orientdb;

import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.InconsistentStateException;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.intel.icecp.module.storage.persistence.providers.LegacyStorageProvider;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Legacy OrientDb storage provider implementation. This implementation now has been altered to co-exist with the tagged
 * provider; TODO eventually they should be merged. This still retains the concept of sessions collecting messages. Each
 * message received and stored in a session will receive a unique, incrementing ID; note that this is a change from
 * previous versions, where the message ID was a hash of the message content (and thus non-incrementing) and multiple
 * messages with the saved content would only be saved once--now each message received will be stored with its own ID.
 * <p>
 * Sessions still exist as a separate vertex in the graph but eventually should be generalized as tags; when this
 * happens the logic for connecting related tags will have to change (finding previously renamed sessions from the
 * current one).
 *
 * @see LegacyStorageProvider
 */
class LegacyOrientDbStorageProvider implements LegacyStorageProvider {
    private static final Logger LOGGER = LogManager.getLogger();

    private final SecureRandom sessionIdGenerator;
    OrientGraph graphDbInstance;

    LegacyOrientDbStorageProvider() {
        this(GraphDbUtils.getGraphDbInstance(new OrientDbConfiguration()));
    }

    LegacyOrientDbStorageProvider(OrientGraph graphDbInstance) {
        sessionIdGenerator = new SecureRandom();
        this.graphDbInstance = graphDbInstance;
    }

    private static int retrieveMaxBufferSizeInSec(Vertex sessionVertex) {
        Integer maxBufferSizeInSecFromOldSession = sessionVertex
                .getProperty(OrientDbNamespace.SESSION_MAX_BUFFER_PERIOD_IN_SEC_KEY);
        return maxBufferSizeInSecFromOldSession != null ? maxBufferSizeInSecFromOldSession
                : StorageModule.DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC;
    }


    // Query a set of Session Id Collection based on the vertex.
    private void getLinkedSessionIds(Vertex v, List<Long> sessionIdList, Set<Collection<Long>> sessionIdSet, boolean onlyWithActiveMessage) {
        if (!v.getEdges(Direction.BOTH, OrientDbNamespace.SESSION_SESSION_RELATIONSHIP).iterator().hasNext()) {
            LOGGER.debug("The first session vertex in this collection: " + v);
            checkToAddSessionId(v, sessionIdList, onlyWithActiveMessage);

            sessionIdSet.add(sessionIdList);
        } else {
            v.getEdges(Direction.OUT, OrientDbNamespace.SESSION_SESSION_RELATIONSHIP).forEach(e -> {
                Vertex fromSessionVertex = e.getVertex(Direction.OUT);
                Vertex toSessionVertex = e.getVertex(Direction.IN);
                // TODO: refactor this into different functions
                if (isRootVertexInSessionLink(fromSessionVertex)) {
                    // always add the very first session in the linked list
                    sessionIdList.add(fromSessionVertex.getProperty(OrientDbNamespace.SESSION_ID_KEY));
                }
                if (toSessionVertex != null) {
                    LOGGER.debug("The current sessionVertex is " + v + " and the next session vertex in this collection: " + toSessionVertex);
                    // the latest is at the beginning of the list
                    checkToAddSessionId(toSessionVertex, sessionIdList, onlyWithActiveMessage);
                    // check if this nextSessionVertex is the last in the linked list
                    if (isLeafVertexInSessionLink(toSessionVertex))
                        sessionIdSet.add(new LinkedList<>(sessionIdList));
                    else
                        getLinkedSessionIds(toSessionVertex, sessionIdList, sessionIdSet, onlyWithActiveMessage);
                } else {
                    LOGGER.debug("the current vertex is leaf vertex: " + v + " and the session linked list is " + sessionIdList);
                    sessionIdSet.add(new LinkedList<>(sessionIdList));
                }

            });
        }
    }

    private void checkToAddSessionId(Vertex v, List<Long> sessionIdList, boolean onlyWithActiveMessage) {
        long sessionId = v.getProperty(OrientDbNamespace.SESSION_ID_KEY);
        if(!onlyWithActiveMessage || getSessionSize(sessionId) > 0) {
            sessionIdList.add(sessionId);
        }
    }

    private static boolean isMidVertexInSessionLink(Vertex sessionVertex) {
        return sessionVertex != null
                && sessionVertex.getEdges(Direction.IN, OrientDbNamespace.SESSION_SESSION_RELATIONSHIP).iterator().hasNext()
                && sessionVertex.getEdges(Direction.OUT, OrientDbNamespace.SESSION_SESSION_RELATIONSHIP).iterator().hasNext();
    }

    private static boolean isLeafVertexInSessionLink(Vertex sessionVertex) {
        return sessionVertex != null
                && !sessionVertex.getEdges(Direction.OUT, OrientDbNamespace.SESSION_SESSION_RELATIONSHIP).iterator().hasNext();
    }

    private static boolean isRootVertexInSessionLink(Vertex sessionVertex) {
        return sessionVertex != null
                && !sessionVertex.getEdges(Direction.IN, OrientDbNamespace.SESSION_SESSION_RELATIONSHIP).iterator().hasNext();
    }

    private static Vertex getVertex(Vertex sessionVertex, Direction edgeDirection, Direction vertexDirection) {
        // Add vertex links between sessions since current session is deleted
        Vertex vertex = null;
        for (Edge e : sessionVertex.getEdges(edgeDirection, OrientDbNamespace.SESSION_SESSION_RELATIONSHIP)) {
            vertex = e.getVertex(vertexDirection);
        }
        return vertex;
    }

    private static void logSessionIdNotFound(long sessionId) {
        LOGGER.warn("sessionId {} does not exist!", sessionId);
    }

    private static long getCutoffTimestamp(final int bufferSize) {
        return System.currentTimeMillis() - bufferSize * 1000L;
    }

    private static void getMessagesFromSession(Vertex session, List<PersistentMessage> msgList) {
        msgList.addAll(StreamSupport.stream(session.getEdges(Direction.OUT, OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP).spliterator(), false)
                .map(e -> e.getVertex(Direction.IN)).filter(x -> x != null)
                .map(PersistentMessageHelper::fromVertex).collect(Collectors.toList()));
    }

    private static void getFilteredMessagesFromSession(Vertex sessionVertex, List<PersistentMessage> msgList) {
        final int bufferSize = retrieveMaxBufferSizeInSec(sessionVertex);
        final long cutoffTimestamp = getCutoffTimestamp(bufferSize);
        msgList.addAll(
                StreamSupport.stream(sessionVertex.getEdges(Direction.OUT, OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP).spliterator(), false)
                        .map(e -> e.getVertex(Direction.IN)).filter(x -> x != null)
                        .filter(v -> !hasTag(v, OrientDbNamespace.INACTIVE_TAG))
                        .map(PersistentMessageHelper::fromVertex)
                        // only collects it when the timestamp is newer
                        .filter(pm -> pm.getTimestamp() >= cutoffTimestamp).collect(Collectors.toList()));
    }

    private static void getFilteredMessagesFromSession(Vertex sessionVertex, int limit, int offset,
                                                       List<PersistentMessage> msgList) {
        final int bufferSize = retrieveMaxBufferSizeInSec(sessionVertex);
        final long cutoffTimestamp = getCutoffTimestamp(bufferSize);
        sessionVertex.getEdges(Direction.OUT, OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP).forEach(e -> {
            Vertex persistentMessageVertex = e.getVertex(Direction.IN);
            if (persistentMessageVertex != null) {
                int edgeIndex = e.getProperty(OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP_INDEX);
                if (edgeIndex >= offset && msgList.size() < limit && !hasTag(persistentMessageVertex, OrientDbNamespace.INACTIVE_TAG)) {
                    PersistentMessage pm = PersistentMessageHelper
                            .fromVertex(persistentMessageVertex);
                    if (pm.getTimestamp() >= cutoffTimestamp) {
                        // only collects it when the timestamp of message is
                        // newer
                        msgList.add(pm);
                    }
                }
            }
        });
    }

    private static boolean hasTag(Vertex vertex, String tag) {
        return StreamSupport.stream(vertex.getVertices(Direction.OUT, OrientDbNamespace.MESSAGE_TAG_RELATIONSHIP).spliterator(), false)
                .anyMatch(v -> tag.equals(v.getProperty(OrientDbNamespace.TAG_NAME_PROPERTY)));
    }

    private static Boolean isStartMessagePresent(long startMessageSeqNum, PersistentMessage message) {
        return message.getId() == startMessageSeqNum;
    }

    private static int getNumberOfConnectedMessages(long sessionId, OrientVertex sessionVertex) {
        long totalEdges = sessionVertex.countEdges(Direction.OUT, OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP);
        LOGGER.info("Total number of edges = " + totalEdges + " for sessionId " + sessionId);
        return (int) totalEdges;
    }

    private static int getNumberOfMessagesWithinBufferPeriod(Vertex sessionVertex) {
        List<PersistentMessage> msgList = new ArrayList<>();
        getFilteredMessagesFromSession(sessionVertex, msgList);
        return msgList.size();
    }

    /**
     * {@inheritDoc}
     * Documentation on why activeOnCurrentThread call is being used:
     * http://orientdb.com/docs/2.1/Java-Multi-Threading.html
     */
    @Override
    public void beginTransaction() {
        graphDbInstance.getRawGraph().activateOnCurrentThread();
        graphDbInstance.begin();
    }

    /**
     * {@inheritDoc}
     * Documentation on why activeOnCurrentThread call is being used:
     * http://orientdb.com/docs/2.1/Java-Multi-Threading.html
     */
    @Override
    public void commitTransaction() {
        graphDbInstance.getRawGraph().activateOnCurrentThread();
        graphDbInstance.commit();
    }

    /**
     * {@inheritDoc}
     * Documentation on why activeOnCurrentThread call is being used:
     * http://orientdb.com/docs/2.1/Java-Multi-Threading.html
     */
    @Override
    public void rollbackTransaction() {
        graphDbInstance.getRawGraph().activateOnCurrentThread();
        graphDbInstance.rollback();
    }

    /**
     * Retrieves a set of channel URIs stored in the OrientDb storage. There is
     * no order preserved in this set.
     *
     * @return Set a set of channel URIs.
     */
    @Override
    public Set<URI> getChannels() {
        Set<URI> channelSet = new HashSet<>();
        graphDbInstance.getVerticesOfClass(OrientDbNamespace.SESSION_CLASS)
                .forEach(v -> channelSet.add(v.getProperty(OrientDbNamespace.SESSION_CHANNEL_KEY)));
        return channelSet;
    }

    /**
     * Given a {@code channelName} URI, this method returns a unique number for
     * sessionId. This method guarantees the uniqueness of session Ids stored in
     * the OrientDb storage as session vertices.
     *
     * @param channelName the URI of a channel.
     * @return long number identifier of a unique session.
     */
    @Override
    public synchronized long createSession(URI channelName) throws StorageModuleException {
        return createSession(channelName, StorageModule.DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC);
    }

    /**
     * Given a {@code channelName} URI, this method returns a unique number for
     * sessionId. This method guarantees the uniqueness of session Ids stored in
     * the OrientDb storage as session vertices.
     *
     * @param channelName the URI of a channel.
     * @param maximumBufferingPeriodInSecond maximum buffer size in second that the storage will hold the data.
     * @return long number identifier of a unique session.
     */
    @Override
    public synchronized long createSession(URI channelName, int maximumBufferingPeriodInSecond) throws StorageModuleException {
        long sessionId = 0;

        if (channelName != null && channelName.toString().length() > 0) {
            long vertexMsgCnt = graphDbInstance.countVertices(OrientDbNamespace.SESSION_CLASS);
            LOGGER.info("Total session class entries = {}", vertexMsgCnt);

            try {
                // check generated sessionId that is NOT in the current database
                do {
                    sessionId = sessionIdGenerator.nextLong();
                } while (getSessionVertexById(sessionId) != null ||
                        (sessionId == 0));

                // make sure the buffer size is positive
                int bufferSize = maximumBufferingPeriodInSecond > 0 ? maximumBufferingPeriodInSecond
                        : StorageModule.DEFAULT_MAX_BUFFERING_PERIOD_IN_SEC;

                // save new sessionId as session vertex
                OrientVertex newSessionVertex = graphDbInstance.addVertex(OrientDbNamespace.SESSION_VERTEX_CLASS_NAME,
                        OrientDbNamespace.SESSION_CHANNEL_KEY, channelName,
                        OrientDbNamespace.SESSION_ID_KEY, sessionId,
                        OrientDbNamespace.SESSION_NEXT_INDEX_KEY, 0,
                        OrientDbNamespace.SESSION_MAX_BUFFER_PERIOD_IN_SEC_KEY, bufferSize);
                LOGGER.debug("New session vertex added with VertexId: {}, maximumBufferingPeriodInSecond = {}",
                        newSessionVertex.getId(), bufferSize);
            } catch (Exception e) {
                throw new StorageModuleException("Found exception while creating session with channelName" + channelName, e);
            }
        }
        return sessionId;
    }

    /**
     * Given a {@code channelName} URI, this method returns the most recent
     * session Id.
     *
     * @param channelName the URI of a channel.
     * @return long number identifier of a unique session.
     */
    @Override
    public synchronized long getLatestActiveSession(URI channelName) throws StorageModuleException {
        Iterable<Vertex> existingSessions =  graphDbInstance.getVertices(OrientDbNamespace.SESSION_CHANNEL_KEY, channelName);
        if (existingSessions.iterator().hasNext()) {
            for (Vertex session: existingSessions) {
                if(isRootVertexInSessionLink(session)) {
                    return (long) session.getProperty(OrientDbNamespace.SESSION_ID_KEY);
                }
            }
        }
        return 0;
    }

    @Override
    public synchronized long renameSession(URI newChannelName, long sessionId) throws StorageModuleException {
        OrientVertex oldSessionVertex = getSessionVertexById(sessionId);
        if (oldSessionVertex == null) {
            throw new StorageModuleException(
                    String.format("The old session vertex is missing for sessionId: %d!", sessionId));
        }

        // retrieve the existing maxBufferingPeriodInSec from session vertex if
        // any
        // and set it to the new renamed session
        int maximumBufferingPeriodInSecond = retrieveMaxBufferSizeInSec(oldSessionVertex);

        // Add new vertex to link previous session to new session.
        long newSessionId = createSession(newChannelName, maximumBufferingPeriodInSecond);

        OrientVertex newSessionVertex = getSessionVertexById(newSessionId);
        if (newSessionVertex == null) {
            throw new StorageModuleException(
                    String.format("The new session vertex is missing for sessionId: %d!", newSessionId));
        }

        LOGGER.info("Adding edge between new session {} -> old session {}.", newSessionId, sessionId);

        try {
            // Add new edge to link the new and old session.
            newSessionVertex.addEdge(OrientDbNamespace.SESSION_SESSION_RELATIONSHIP, oldSessionVertex);
        } catch (IllegalArgumentException | ORecordNotFoundException ex) {
            graphDbInstance.removeVertex(newSessionVertex);
            throw new StorageModuleException(
                    String.format("Error when adding edge between new session %d -> old session %d. %s", newSessionId,
                            sessionId, ex));
        }

        return newSessionId;
    }

    /**
     * Given a {@code channelName} URI, this method returns the set of
     * collection with linked session identifiers, associating with that channel
     * URI, which are currently stored in the OrientDb storage as session
     * vertices.
     *
     * @param channelName the URI of a channel.
     * @return a set of collection of long number identifiers for linked sessions.
     * @see #createSession(URI)
     */
    @Override
    public Set<Collection<Long>> getSessions(URI channelName) {

        Set<Collection<Long>> sessionIdCollection = new HashSet<>();
        if (channelName != null && channelName.toString().length() > 0) {
            Iterable<Vertex> allVertices = graphDbInstance.getVertices(OrientDbNamespace.SESSION_CHANNEL_VERTEX_KEY, channelName);
            StreamSupport.stream(allVertices.spliterator(), false)
                    .filter(vertex -> !isMidVertexInSessionLink(vertex))
                    .forEach(vertex -> {
                        LinkedList<Long> sessionIdList = new LinkedList<>();
                        LOGGER.debug("Found vertex with channelName [{}]: {}", channelName, vertex);
                        getLinkedSessionIds(vertex, sessionIdList, sessionIdCollection, false);
                    });
        }

        return sessionIdCollection;
    }

    /**
     * Given a {@code querySessionId}, this method returns the set of collection
     * with linked session identifiers, associating with that querySessionId,
     * which are currently stored in the OrientDb storage as session vertices.
     * Note: the input to the command should be an "active" session identifier
     *
     * @param querySessionId an active session identifier.
     * @return a set of collection of long number identifiers for linked sessions.
     * @see #createSession(URI)
     */
    @Override
    public Set<Collection<Long>> getSessions(long querySessionId) {

        return getSessions(querySessionId, false);
    }

    private Set<Collection<Long>> getSessions(long querySessionId, boolean onlyWithActiveMessage) {
        Set<Collection<Long>> sessionIdCollection = new HashSet<>();
        if (querySessionId != 0L) {
            Iterable<Vertex> allVertices = graphDbInstance.getVertices(OrientDbNamespace.SESSION_ID_VERTEX_KEY, querySessionId);
            StreamSupport.stream(allVertices.spliterator(), false)
                    .filter(vertex -> !isMidVertexInSessionLink(vertex))
                    .forEach(vertex -> {
                        LinkedList<Long> sessionIdList = new LinkedList<>();
                        LOGGER.debug("Found vertex with querySessionId [{}]: {}", querySessionId, vertex);
                        getLinkedSessionIds(vertex, sessionIdList, sessionIdCollection, onlyWithActiveMessage);
                    });
        }
        return sessionIdCollection;
    }

    /**
     * Given a {@code querySessionId}, this method returns the set of collection
     * with linked session identifiers for those having active messages, associating with
     * that querySessionId, which are currently stored in the OrientDb storage as session vertices.
     * Note: the input to the command should be an "active" session identifier
     *
     * @param querySessionId an active session identifier.
     * @return a set of collection of long number identifiers for linked sessions with active messages.
     * @see #createSession(URI)
     */
    @Override
    public Set<Collection<Long>> getSessionsWithActiveMessages(long querySessionId) {

        return getSessions(querySessionId, true);
    }

    /**
     * Given a {@code sessionId}, this method deletes the session identifier
     * currently stored in the OrientDb storage as session vertex type. If there
     * is any orphaned persistent message under the deleting session, that
     * persistent message is also to be deleted.
     *
     * @param sessionId the session identifier.
     * @see #createSession(URI)
     */
    @Override
    public synchronized void deleteSession(long sessionId) throws StorageModuleException {
        doesSessionIdExist(sessionId);

        OrientVertex sessionVertex = getSessionVertexById(sessionId);
        if (sessionVertex == null) {
            throw new StorageModuleException(
                    String.format("The vertex is missing for sessionId: %d!", sessionId));
        }

        // Remove connected messages
        sessionVertex.getEdges(Direction.OUT, OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP).forEach(e -> {
            Vertex persistentMessageVertex = e.getVertex(Direction.IN);
            graphDbInstance.removeEdge(e);
            // Remove message vertex if it becomes orphan and not referred
            // to by other sessions
            if (persistentMessageVertex != null
                    && !persistentMessageVertex.getEdges(Direction.IN, OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP).iterator().hasNext()) {
                graphDbInstance.removeVertex(persistentMessageVertex);
            }
        });

        // Add vertex links between sessions since current session is
        // deleted
        Vertex left = getVertex(sessionVertex, Direction.IN, Direction.OUT);
        Vertex right = getVertex(sessionVertex, Direction.OUT, Direction.IN);

        if (left != null && right != null) {
            try {
                left.addEdge(OrientDbNamespace.SESSION_SESSION_RELATIONSHIP, right);
            } catch (IllegalArgumentException | ORecordNotFoundException e) {
                throw new StorageModuleException(String.format(
                        "The new session edge could not be added between vertices %s -> %s. Error: %s ",
                        left.getId().toString(), right.getId().toString(), e));
            }
        } else {
            LOGGER.info("Vertex with sessionId {}:{} is a leaf vertex. ", sessionId, sessionVertex);
        }

        graphDbInstance.removeVertex(sessionVertex);
    }

    /**
     * Given a {@code sessionId} and a specific {@code persistentMessage}, this
     * method saves the specific message into the OrientDb storage as
     * persistentMessage vertex type. If there is some missing session vertex
     * that is supposed to be associated with {@code sessionId}, the
     * {@code InconsistentStateException} will be thrown. If there are any
     * messages older than the maximum buffering period in second from now, then
     * these messages will be also cleaned up from the storage.
     *
     * @param sessionId the session identifier.
     * @param persistentMessage a message to be persisted.
     * @return message Id
     * @throws StorageModuleException when unable to save message
     */
    @Override
    public synchronized long saveMessage(long sessionId, PersistentMessage persistentMessage) throws StorageModuleException {
        if (persistentMessage == null) {
            throw new StorageModuleException("Failed to save message; attempted to save a null message, aborting");
        }

        doesSessionIdExist(sessionId);

        try {
            long id = nextMessageId();
            persistentMessage.setId(id);

            OrientVertex persistentMessageVertex = graphDbInstance.addVertex("class:" + OrientDbNamespace.MESSAGE_CLASS,
                    OrientDbNamespace.MESSAGE_ID_PROPERTY, persistentMessage.getId(),
                    OrientDbNamespace.MESSAGE_TIMESTAMP_PROPERTY, persistentMessage.getTimestamp(),
                    OrientDbNamespace.MESSAGE_CONTENT_PROPERTY, persistentMessage.getMessageContent());
            LOGGER.info("New PersistentMessage vertex added with VertexId: {}", persistentMessageVertex.getId());

            OrientVertex sessionVertex = getSessionVertexById(sessionId);
            if (sessionVertex == null) {
                throw new InconsistentStateException("The session vertex is missing for session ID: " + sessionId);
            }

            // clean up all the messages that are outside maximum
            // buffering period
            // time window comparing to the timestamp of the message
            cleanupMessagesOlderThanBufferPeriod(sessionId, sessionVertex);

            LOGGER.info("Adding edge from session {} to message {}", sessionId, persistentMessage);
            Edge e = sessionVertex.addEdge(OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP, persistentMessageVertex);
            // setup nextIndex for this edge
            Integer nextIndex = sessionVertex.getProperty(OrientDbNamespace.SESSION_NEXT_INDEX_KEY);
            e.setProperty(OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP_INDEX, nextIndex++);
            sessionVertex.setProperty(OrientDbNamespace.SESSION_NEXT_INDEX_KEY,
                    nextIndex);
            return id;
        } catch (Exception e) {
            throw new StorageModuleException(String.format("Found exception while saving PersistentMessage data with sessionId %d",
                    sessionId), e);
        }
    }

    private long nextMessageId() {
        OSequence idSequence = graphDbInstance.getRawGraph().getMetadata().getSequenceLibrary().getSequence(OrientDbNamespace.ID_SEQUENCE);
        return idSequence.next();
    }

    private void cleanupMessagesOlderThanBufferPeriod(long sessionId, OrientVertex sessionVertex) throws StorageModuleException {
        final int bufferSize = retrieveMaxBufferSizeInSec(sessionVertex);
        final long cutoffTimestamp = getCutoffTimestamp(bufferSize);
        List<PersistentMessage> msgListPerSession = new ArrayList<>();
        getMessagesFromSession(sessionVertex, msgListPerSession);
        for (PersistentMessage msg : msgListPerSession) {
            long msgTimestamp = msg.getTimestamp();
            // delete messages older than cutoff time
            if (msgTimestamp < cutoffTimestamp) {
                LOGGER.info(
                        "Cleanup message older than {} by maximum buffering period in {} seconds for sessionId {}. Message: {}",
                        cutoffTimestamp, bufferSize, sessionId, msg);
                try {
                    deleteMessage(sessionId, msg.getId());
                } catch (StorageModuleException e) {
                    LOGGER.warn("Failed to clean up message!  sessionId {} message: {}", sessionId, msg, e);
                }
            }
        }
    }

    /**
     * Given a {@code sessionId}, this method gets the list of persisted
     * messages stored in the OrientDb storage as persistentMessage vertex type.
     * The timestamps of messages should be within the maximum buffering period
     * in seconds from now.
     *
     * @param sessionId the session identifier.
     * @return a list of persistent messages stored in the database for the particular session identifier.
     * @see #getMessages(long, int, int)
     */
    @Override
    public List<PersistentMessage> getMessages(long sessionId) throws StorageModuleException {
        List<PersistentMessage> msgList = new ArrayList<>();
        doesSessionIdExist(sessionId);
        graphDbInstance.getVertices(OrientDbNamespace.SESSION_ID_VERTEX_KEY, sessionId)
                .forEach(s -> getFilteredMessagesFromSession(s, msgList));

        return msgList;
    }

    /**
     * Given a {@code sessionId}, {@code limit}, and {@code offset}, this method
     * gets the list of persisted messages for that criteria stored in the
     * OrientDb storage as persistentMessage vertex type. The timestamps of
     * messages should be within the maximum buffering period in seconds from
     * now.
     *
     * @param sessionId the session identifier.
     * @param limit maximum number of messages to get. This should be a positive number.
     * @param offset the index or offset of the persistent message in the list to start with.
     * @return a list of satisfied criteria persistent messages stored in the database for the particular session
     * identifier.
     * @see #getMessages(long)
     */
    @Override
    public List<PersistentMessage> getMessages(long sessionId, int limit, int offset) throws StorageModuleException {
        List<PersistentMessage> msgList = new ArrayList<>();
        if (limit <= 0 || offset < 0) {
            return msgList;
        }

        doesSessionIdExist(sessionId);
        graphDbInstance.getVertices(OrientDbNamespace.SESSION_ID_VERTEX_KEY, sessionId).forEach(v -> {
            int nextIndex = v.getProperty(OrientDbNamespace.SESSION_NEXT_INDEX_KEY);
            if (nextIndex > 0 && offset < nextIndex) {
                getFilteredMessagesFromSession(v, limit, offset, msgList);
            }
        });

        return msgList;
    }

    /**
     * {@inheritDoc}
     *
     * @see #getMessages(long)
     * @see #deleteMessage(long, long)
     */
    @Override
    public synchronized void deleteMessagesByRange(long sessionId, long startMessageSeqNum, long endMessageSeqNum)
            throws StorageModuleException {
        List<PersistentMessage> sessionMessages = getMessages(sessionId);

        boolean isStartMessageFound = false;

        sessionMessages.sort((PersistentMessage m1, PersistentMessage m2) -> Long
                .compare(m1.getId(), m2.getId()));

        for (PersistentMessage message : sessionMessages) {
            // find the provided starting message
            if (!isStartMessageFound) {
                isStartMessageFound = isStartMessagePresent(startMessageSeqNum, message);
            }

            if (isStartMessageFound) {
                // delete all messages, until we find the ending message.
                deleteMessage(sessionId, message.getId());
                if (message.getId() == endMessageSeqNum) {
                    return;
                }
            }
        }

        if (!isStartMessageFound) {
            throw new StorageModuleException(
                    String.format("DeleteMessagesByRange could not find the starting message sequence number: %d",
                            startMessageSeqNum));
        }

        // If we get here, then we never found the ending message.
        throw new StorageModuleException(String.format(
                "DeleteMessagesByRange could not find the ending message sequence number: %d", endMessageSeqNum));

    }

    /**
     * Given a {@code sessionId}, this method deletes the list of persisted
     * messages for that session stored in the OrientDb storage.
     *
     * @param sessionId the session identifier.
     * @return boolean to indicate whether the persistent messages are deleted for the particular session identifier. It
     * returns false if {@code sessionId} does not exist in the storage.
     * @see #deleteMessage(long, long)
     */
    @Override
    public synchronized boolean deleteMessages(long sessionId) throws StorageModuleException {
        boolean ok = false;
        OrientVertex sessionVertex = getSessionVertexById(sessionId);
        if (sessionVertex != null) {
            try {
                getNumberOfConnectedMessages(sessionId, sessionVertex);
                sessionVertex.getEdges(Direction.OUT, OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP).forEach(e -> {
                    Vertex persistentMessageVertex = e.getVertex(Direction.IN);
                    graphDbInstance.removeEdge(e);
                    // remove message vertex if it becomes orphan (eg. not
                    // referred by other session vertices):
                    if (persistentMessageVertex != null && !persistentMessageVertex
                            .getEdges(Direction.BOTH, OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP).iterator().hasNext()) {
                        graphDbInstance.removeVertex(persistentMessageVertex);
                    }
                });
                ok = true;
            } catch (Exception e) {
                throw new StorageModuleException(String.format("Failed to delete messages with sessionId {}: %d",
                        sessionId), e);
            }
        } else {
            logSessionIdNotFound(sessionId);
        }
        return ok;
    }

    /**
     * Given a {@code sessionId}, and the specific {@code persistentMessageId},
     * this method deletes the persisted message for that session stored in the
     * OrientDb storage.
     *
     * @param sessionId the session identifier.
     * @param messageId the channel sequence number for a persistent message.
     * @see #deleteMessages(long)
     */
    @Override
    public synchronized void deleteMessage(long sessionId, long messageId) throws StorageModuleException {
        if (messageId < 0) {
            throw new StorageModuleException("Channel Sequence Number is negative!");
        }

        OrientVertex sessionVertex = getSessionVertexById(sessionId);
        if (sessionVertex != null) {
            try {
                getNumberOfConnectedMessages(sessionId, sessionVertex);

                sessionVertex.getEdges(Direction.OUT, OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP)
                        .forEach(e -> deleteSessionMessage(e, messageId));
            } catch (Exception e) {
                throw new StorageModuleException(String.format("Failed to delete messages with sessionId {}: %d",
                        sessionId), e);
            }
        } else {
            logSessionIdNotFound(sessionId);
        }
    }

    private void deleteSessionMessage(Edge messageEdge, long messageId) {
        Vertex persistentMessageVertex = messageEdge.getVertex(Direction.IN);

        if (persistentMessageVertex != null && (long) persistentMessageVertex.getProperty(OrientDbNamespace.MESSAGE_ID_PROPERTY) == messageId) {
            LOGGER.debug("Removing edge: {}", messageEdge);
            graphDbInstance.removeEdge(messageEdge);
            // remove message vertex if it becomes orphan (eg. not
            // referred by other session vertices):
            if (!persistentMessageVertex.getEdges(Direction.BOTH, OrientDbNamespace.SESSION_MESSAGE_RELATIONSHIP).iterator().hasNext()) {
                LOGGER.debug("Removing the orphaned persistent message vertex {}", persistentMessageVertex);
                graphDbInstance.removeVertex(persistentMessageVertex);
            }
        }
    }

    /**
     * Given a {@code sessionId}, this method returns the number of persisted
     * messages currently stored for that session in the OrientDb storage.
     *
     * @param sessionId the session identifier.
     * @return an integer number to indicate the total number of persistent messages for the particular session
     * identifier. It returns {@code 0} if {@code sessionId} does not exist in the storage.
     * @see #getMessages(long)
     */
    @Override
    public int getSessionSize(long sessionId) {
        int numOfMessages = 0;
        OrientVertex sessionVertex = getSessionVertexById(sessionId);
        if (sessionVertex != null) {
            numOfMessages = getNumberOfMessagesWithinBufferPeriod(sessionVertex);
        } else {
            logSessionIdNotFound(sessionId);
        }
        return numOfMessages;
    }

    /**
     * Get the previous linked session id. This method is used during
     * DeleteSession of currently active session, to retrieve previous linked
     * session and make it active
     *
     * @param sessionId the current session id
     * @return the previous linked session id
     */
    @Override
    public long getPreviousSession(long sessionId) {
        OrientVertex sessionVertex = getSessionVertexById(sessionId);
        if (sessionVertex == null) {
            logSessionIdNotFound(sessionId);
            return 0L;
        }

        Vertex right = null;

        for (Edge e : sessionVertex.getEdges(Direction.OUT, OrientDbNamespace.SESSION_SESSION_RELATIONSHIP)) {
            right = e.getVertex(Direction.IN);
        }
        if (right == null) {
            return 0L;
        }

        return right.getProperty(OrientDbNamespace.SESSION_ID_KEY);
    }

    /**
     * Shuts down the current graph database instance to clean up the resources.
     * The shutdown instance cannot be re-used any more.
     */
    public synchronized void shutdown() {
        GraphDbUtils.shutdownDbInstance(graphDbInstance);
    }

    void doesSessionIdExist(long sessionId) throws StorageModuleException {
        if (!graphDbInstance.getVertices(OrientDbNamespace.SESSION_ID_VERTEX_KEY, sessionId).iterator().hasNext()) {
            throw new StorageModuleException(String.format(
                    "SessionId %d does not exist!", sessionId));
        }
    }

    OrientVertex getSessionVertexById(long sessionId) {
        OrientVertex v = null;
        Iterator<Vertex> sessionVertex = graphDbInstance.getVertices(OrientDbNamespace.SESSION_ID_VERTEX_KEY, sessionId).iterator();
        if (sessionVertex.hasNext()) {
            v = (OrientVertex) sessionVertex.next();
        }
        return v;
    }
}
