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

import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.module.query.And;
import com.intel.icecp.module.query.Id;
import com.intel.icecp.module.query.Query;
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.exceptions.TaggingOperationException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.intel.icecp.module.storage.persistence.providers.QueriesStorageProvider;
import com.intel.icecp.module.storage.persistence.providers.TaggedStorageProvider;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class implements the tagged storage API; the number of methods is limited only to those used for
 * tagging-specific functionality. Any custom queries should be implemented under {@link QueriesStorageProvider}.
 * <p>
 * Also, this class assumes queries that are single-level AND operators. More complex queries could be added in the
 * future but this limitation is due to the current requirements for the storage module.
 * <p>
 * More information on OrientDB at http://orientdb.com/docs/2.1/Tutorial-Java.html
 *
 */
class TaggedOrientDbStorageProvider implements TaggedStorageProvider {
    private static final Logger LOGGER = LogManager.getLogger();
    private final OrientGraph db;

    TaggedOrientDbStorageProvider(OrientGraph graphDatabase) {
        this.db = graphDatabase;
    }

    /**
     * Get the minimum timestamp of all "active" messages on the given {@code channelName}
     *
     * @param channelName the name of the channel to search in
     * @return the smallest timestamp found on a message persisted on this channel
     * @throws TaggingOperationException if the operation fails
     */
    @Override
    public long getActiveMinimumTimestamp(String channelName) throws TaggingOperationException {
        OCommandSQL minQuery = new OCommandSQL("SELECT min(" + OrientDbNamespace.MESSAGE_TIMESTAMP_PROPERTY + ") " +
                "FROM (SELECT expand(intersect((SELECT expand(in()) FROM " + OrientDbNamespace.TAG_CLASS + " WHERE name = ?), " +
                "(SELECT expand(@rid) FROM " + OrientDbNamespace.MESSAGE_CLASS + " WHERE ? NOT IN out(?).name))))");

        Iterable<Vertex> result = db.command(minQuery).execute(
                channelName,
                OrientDbNamespace.INACTIVE_TAG,
                OrientDbNamespace.MESSAGE_TAG_RELATIONSHIP);

        Iterator<Vertex> it = result.iterator();
        return it.hasNext() ? it.next().getProperty("min") : 0L;
    }

    /**
     * Get the maximum timestamp of all "active" messages on the given {@code channelName}
     *
     * @param channelName the name of the channel to search in
     * @return the largest timestamp found on a message persisted on this channel
     * @throws TaggingOperationException if the operation fails
     */
    @Override
    public long getActiveMaximumTimestamp(String channelName) throws TaggingOperationException {
        OCommandSQL maxQuery = new OCommandSQL("SELECT max(" + OrientDbNamespace.MESSAGE_TIMESTAMP_PROPERTY + ") " +
                "FROM (SELECT expand(intersect((SELECT expand(in()) FROM " + OrientDbNamespace.TAG_CLASS + " WHERE name = ?), " +
                "(SELECT expand(@rid) FROM " + OrientDbNamespace.MESSAGE_CLASS + " WHERE ? NOT IN out(?).name))))");

        Iterable<Vertex> result = db.command(maxQuery).execute(
                channelName,
                OrientDbNamespace.INACTIVE_TAG,
                OrientDbNamespace.MESSAGE_TAG_RELATIONSHIP);

        Iterator<Vertex> it = result.iterator();
        return it.hasNext() ? it.next().getProperty("max") : 0L;
    }

    @Override
    public synchronized Id add(BytesMessage message) throws TaggingOperationException {
        try {
            db.begin();

            long id = nextId();
            Vertex v = db.addVertex("class:" + OrientDbNamespace.MESSAGE_CLASS,
                    OrientDbNamespace.MESSAGE_ID_PROPERTY, id,
                    OrientDbNamespace.MESSAGE_CONTENT_PROPERTY, message.getBytes(),
                    OrientDbNamespace.MESSAGE_TIMESTAMP_PROPERTY, System.currentTimeMillis());

            db.commit();

            LOGGER.info("New {} vertex added with id: {}", OrientDbNamespace.MESSAGE_CLASS, v.getId());
            return new Id(id);
        } catch (Exception e) {
            throw new TaggingOperationException("Failed to add new vertex", e);
        }
    }

    @Override
    public synchronized Set<PersistentMessage> find(Query query) throws TaggingOperationException {
        try {
            return selectMessagesFromQuery(query).map(PersistentMessageHelper::fromVertex).collect(Collectors.toSet());
        } catch (Exception e) {
            throw new TaggingOperationException("Failed to execute query to find messages", e);
        }
    }

    @Override
    public synchronized Set<PersistentMessage> remove(Query query) throws TaggingOperationException {
        try {
            Set<PersistentMessage> messages = selectMessagesFromQuery(query).map(v -> {
                PersistentMessage pm = PersistentMessageHelper.fromVertex(v);
                v.remove();
                return pm;
            }).collect(Collectors.toSet());

            LOGGER.info("Removed {} message(s)", messages.size());

            return Collections.unmodifiableSet(messages);
        } catch (Exception e) {
            throw new TaggingOperationException("Failed to execute query to remove messages", e);
        }
    }

    @Override
    public synchronized Set<Id> tag(Query query, Tag tag) throws TaggingOperationException {
        try {
            Vertex tagVertex = createAndSelectTag(tag);
            LOGGER.debug("tagVertex {}", tagVertex);
            Set<Id> tagged = new LinkedHashSet<>();
            long count = selectMessagesFromQuery(query).map(v -> {
                long id = v.getProperty(OrientDbNamespace.MESSAGE_ID_PROPERTY);
                tagged.add(new Id(id));
                // see documentation at http://orientdb.com/docs/2.1/SQL-Create-Edge.html
                return v.addEdge(OrientDbNamespace.MESSAGE_TAG_RELATIONSHIP, tagVertex);
            }).count();

            LOGGER.info("Tagged {} message(s) with tag {}", count, tag);

            return Collections.unmodifiableSet(tagged);
        } catch (Exception e) {
            throw new TaggingOperationException("Failed to tag items with tag " + tag, e);
        }
    }

    @Override
    public synchronized Set<Id> untag(Query query, Tag tag) throws TaggingOperationException {
        try {
            OrientVertex tagVertex = (OrientVertex) createAndSelectTag(tag);
            LOGGER.debug("tagVertex: {}", tagVertex);
            Set<Id> untagged = new LinkedHashSet<>();
            long count = selectMessagesFromQuery(query).map(v -> {
                untagged.add(new Id(v.getProperty(OrientDbNamespace.MESSAGE_ID_PROPERTY)));
                // use graph API for retrieving edges to the tag
                Spliterator<Edge> spliterator = ((OrientVertex) v).getEdges(tagVertex, Direction.OUT).spliterator();
                // see documentation at http://orientdb.com/docs/2.1/SQL-Delete-Edge.html
                StreamSupport.stream(spliterator, false).forEach(Edge::remove);
                return 1;
            }).count();

            removeTagIfUnused(tag);

            LOGGER.info("Untagged {} message(s) with tag {}", count, tag);

            return Collections.unmodifiableSet(untagged);
        } catch (Exception e) {
            throw new TaggingOperationException("Failed to untag items with tag " + tag, e);
        }
    }

    @Override
    public synchronized Set<Tag> related(Tag... tags) throws TaggingOperationException {
        String osql = "SELECT expand(distinct(set(in().out()))) FROM Tag WHERE name = ?"; // TODO work with multiple tags
        Object[] params = Arrays.stream(tags).map(Tag::value).toArray(Object[]::new);
        LOGGER.info("Finding tags related to {} using query '{}'", params, osql);

        OCommandSQL cmd = new OCommandSQL(osql);
        Iterable<Vertex> related = db.command(cmd).execute(params);

        return StreamSupport.stream(related.spliterator(), false).map(v -> new Tag(v.getProperty(OrientDbNamespace.TAG_NAME_PROPERTY))).collect(Collectors.toSet());
    }

    private long nextId() {
        OSequence idSequence = db.getRawGraph().getMetadata().getSequenceLibrary().getSequence(OrientDbNamespace.ID_SEQUENCE);
        return idSequence.next();
    }

    /**
     * Note: this only supports AND-ed conjunctions
     *
     * @param query the set of selection with which to filter the result set
     * @return a stream of vertices filtered by the query
     */
    private Stream<Vertex> selectMessagesFromQuery(Query query) {
        OsqlAnd and = new OsqlAnd(db, (And) query.root());
        return StreamSupport.stream(and.spliterator(), false).map(i -> new OrientVertex(db, i));
    }

    private Vertex createAndSelectTag(Tag tag) {
        Vertex tagVertex = selectTag(tag);
        if (tagVertex == null) {
            tagVertex = db.addVertex("class:" + OrientDbNamespace.TAG_CLASS, OrientDbNamespace.TAG_NAME_PROPERTY, tag.value());
            LOGGER.info("Created tag {} with vertex: {}", tag, tagVertex);
        }
        return tagVertex;
    }

    private Vertex selectTag(Tag tag) {
        Iterable<Vertex> vertices = db.getVertices(OrientDbNamespace.TAG_CLASS, new String[]{OrientDbNamespace.TAG_NAME_PROPERTY}, new Object[]{tag.value()});
        Iterator<Vertex> iterator = vertices.iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }

    private void removeTagIfUnused(Tag tag) {
        Vertex tagVertex = selectTag(tag);
        if (tagVertex != null && !tagVertex.getEdges(Direction.IN).iterator().hasNext()) {
            tagVertex.remove();
            LOGGER.info("Removed unused tag: {}", tag);
        }
    }
}
