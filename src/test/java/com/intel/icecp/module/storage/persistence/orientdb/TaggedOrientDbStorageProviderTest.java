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
import com.intel.icecp.module.query.Id;
import com.intel.icecp.module.query.Queries;
import com.intel.icecp.module.query.Query;
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 */
public class TaggedOrientDbStorageProviderTest {
    private static final Logger LOGGER = LogManager.getLogger();
    private TaggedOrientDbStorageProvider instance;
    private OrientGraph graph;

    @Before
    public void before() {
        OrientDbConfiguration configuration = new OrientDbConfiguration();
        configuration.setStorageType(OrientDbStorageType.IN_MEMORY_GRAPH);
        graph = GraphDbUtils.getGraphDbInstance(configuration);

        OrientDbNamespace.setupSchemata(graph);
        instance = new TaggedOrientDbStorageProvider(graph);
    }

    @After
    public void after() {
        graph.drop();
    }

    @Test
    public void testGetActiveMinTimeReturnsCorrectValue() throws Exception {
        addAndTagSomeMessages(3, "ndn:/foo");

        long min = instance.getActiveMinimumTimestamp("ndn:/foo");
        assertNotEquals(0L, min);

        Set<PersistentMessage> message = instance.find(Queries.fromId(1));
        assertEquals(min, first(message).getTimestamp());
    }

    @Test
    public void testGetActiveMinTimeReturnsZeroWhenAllMessagesInactive() throws Exception {
        addAndTagSomeMessages(3, OrientDbNamespace.INACTIVE_TAG, "ndn:/foo");

        long min = instance.getActiveMinimumTimestamp("ndn:/foo");
        assertEquals(0L, min);
    }

    @Test
    public void testGetActiveMaxTimeReturnsZeroWhenAllMessagesInactive() throws Exception {
        addAndTagSomeMessages(3, OrientDbNamespace.INACTIVE_TAG, "ndn:/foo");

        long max = instance.getActiveMaximumTimestamp("ndn:/foo");
        assertEquals(0L, max);
    }

    @Test
    public void testGetActiveMaxTimeReturnsCorrectValue() throws Exception {
        addAndTagSomeMessages(3, "ndn:/foo");

        long max = instance.getActiveMaximumTimestamp("ndn:/foo");
        assertNotEquals(0L, max);

        Set<PersistentMessage> message = instance.find(Queries.fromId(3));
        assertEquals(max, first(message).getTimestamp());
    }

    @Test
    public void testGetActiveTsReturnsSameValueIfOnlyOneMessageIsPresent() throws Exception {
        addAndTagSomeMessages(2, "ndn:/foo");
        addAndTagSomeMessages(1, "ndn:/bar");

        long min = instance.getActiveMinimumTimestamp("ndn:/bar");
        long max = instance.getActiveMaximumTimestamp("ndn:/bar");

        assertEquals(min, max);
    }

    @Test
    public void testGetActiveTsReturnsZeroIfNoMessageIsPresent() throws Exception {
        long min = instance.getActiveMinimumTimestamp("foo-tag");
        long max = instance.getActiveMaximumTimestamp("foo-tag");
        assertEquals(0L, min);
        assertEquals(max, min);
    }

    @Test
    public void findUsingRelativeTimestamp() throws Exception {
        addAndTagSomeMessages(3, "a");
        Query query = new Query(new com.intel.icecp.module.query.Before(1));

        Set<PersistentMessage> messagesBefore = instance.find(query);
        assertEquals(0, messagesBefore.size());

        Thread.sleep(1001); // 'before' queries only have precision up to the second

        Set<PersistentMessage> messagesAfter = instance.find(query);
        assertEquals(3, messagesAfter.size());
    }

    @Test
    public void findUsingUnknownTags() throws Exception {
        Set<PersistentMessage> messages = instance.find(Queries.fromTags("a", "b"));
        assertEquals(0, messages.size());
    }

    @Test
    public void findUsingUnknownId() throws Exception {
        Set<PersistentMessage> messages = instance.find(Queries.fromId(42));
        assertEquals(0, messages.size());
    }

    @Test
    public void findUsingId() throws Exception {
        instance.add(new BytesMessage(".".getBytes()));
        instance.add(new BytesMessage("..".getBytes()));
        instance.add(new BytesMessage("...".getBytes()));

        Set<PersistentMessage> messages = instance.find(Queries.fromId(1));
        assertEquals(1, messages.size());
    }

    @Test
    public void findUsingTags() throws Exception {
        Id one = instance.add(new BytesMessage(".".getBytes()));
        Id two = instance.add(new BytesMessage("..".getBytes()));
        Id three = instance.add(new BytesMessage("...".getBytes()));
        Id four = instance.add(new BytesMessage("....".getBytes()));
        Id five = instance.add(new BytesMessage(".....".getBytes()));

        instance.tag(new Query(one), new Tag("a"));
        instance.tag(new Query(one), new Tag("d"));
        instance.tag(new Query(two), new Tag("a"));
        instance.tag(new Query(two), new Tag("b"));
        instance.tag(new Query(three), new Tag("d"));
        instance.tag(new Query(four), new Tag("c"));
        instance.tag(new Query(five), new Tag("e"));

        Set<PersistentMessage> messages2 = instance.find(Queries.fromTags("a", "c"));
        assertEquals(0, messages2.size());

        Set<PersistentMessage> messages1 = instance.find(Queries.fromTags("a", "b"));
        assertEquals(1, messages1.size());
        assertEquals((long) two.value(), first(messages1).getId());

        Set<PersistentMessage> messages3 = instance.find(Queries.fromTags("d"));
        assertEquals(2, messages3.size());

        Set<PersistentMessage> messages4 = instance.find(Queries.fromTags("e"));
        assertEquals(1, messages4.size());
        assertEquals((long) five.value(), first(messages4).getId());
    }

    @Test
    public void tagMessages() throws Exception {
        Id one = instance.add(new BytesMessage(".".getBytes()));
        Id two = instance.add(new BytesMessage("..".getBytes()));

        instance.tag(new Query(one), new Tag("a"));
        instance.tag(new Query(two), new Tag("a"));

        Set<PersistentMessage> messages = instance.find(Queries.fromTags("a"));

        assertEquals(2, messages.size());
        assertTrue(contains(messages, "."));
        assertTrue(contains(messages, ".."));
    }

    @Test
    public void untagMessages() throws Exception {
        Id one = instance.add(new BytesMessage(".".getBytes()));
        Id two = instance.add(new BytesMessage("..".getBytes()));

        Tag a = new Tag("a");
        instance.tag(new Query(one), a);
        instance.tag(new Query(two), new Tag("b"));

        Set<Id> untagged = instance.untag(new Query(a), a);

        assertEquals(1, untagged.size());
        assertEquals(one.value(), first(untagged).value());
    }

    @Test
    public void findRelatedTags() throws Exception {
        Id one = instance.add(new BytesMessage(".".getBytes()));
        Id two = instance.add(new BytesMessage("..".getBytes()));

        Tag a = new Tag("a");
        Tag b = new Tag("b");
        Tag c = new Tag("c");

        instance.tag(new Query(one), a);
        instance.tag(new Query(two), b);
        instance.tag(new Query(one), c);
        instance.tag(new Query(two), c);

        Set<Tag> related = instance.related(c);

        assertEquals(3, related.size());
        assertTrue(related.contains(a));
        assertTrue(related.contains(b));
        assertTrue(related.contains(c));
    }

    @Test
    public void removeMessagesWithOneTag() throws Exception {
        Id one = instance.add(new BytesMessage(".".getBytes()));
        Id two = instance.add(new BytesMessage("..".getBytes()));

        Tag a = new Tag("a");

        instance.tag(new Query(one), a);
        instance.tag(new Query(two), a);

        Set<PersistentMessage> deletedMessages = instance.remove(Queries.fromTags("a"));
        assertEquals(2, deletedMessages.size());

    }

    @Test
    public void removeMessagesWithMultipleTags() throws Exception {
        Id one = instance.add(new BytesMessage(".".getBytes()));
        Id two = instance.add(new BytesMessage("..".getBytes()));
        Id three = instance.add(new BytesMessage("...".getBytes()));
        Id four = instance.add(new BytesMessage("...".getBytes()));

        Tag a = new Tag("a");
        Tag b = new Tag("b");
        Tag c = new Tag("c");
        Tag d = new Tag("d");
        Tag e = new Tag("e");

        instance.tag(new Query(one), a);
        instance.tag(new Query(one), b);
        instance.tag(new Query(one), c);
        instance.tag(new Query(two), a);
        instance.tag(new Query(two), c);
        instance.tag(new Query(three), d);
        instance.tag(new Query(three), e);
        instance.tag(new Query(four), d);
        instance.tag(new Query(four), e);

        Set<PersistentMessage> deletedMessages1 = instance.remove(Queries.fromTags("a", "b"));
        assertEquals((long) one.value(), first(deletedMessages1).getId());
        assertEquals(1, deletedMessages1.size());

        Set<PersistentMessage> deletedMessages2 = instance.remove(Queries.fromTags("a", "c"));
        assertEquals((long) two.value(), first(deletedMessages2).getId());
        assertEquals(1, deletedMessages2.size());

        Set<PersistentMessage> deletedMessages3 = instance.remove(Queries.fromTags("d", "e"));
        assertEquals(2, deletedMessages3.size());

    }

    @Test
    public void removeWithNonExistentTags() throws Exception {
        Set<PersistentMessage> deletedMessages1 = instance.remove(Queries.fromTags("x", "y"));
        assertEquals(0, deletedMessages1.size());

        Set<PersistentMessage> deletedMessages2 = instance.remove(Queries.fromTags());
        assertEquals(0, deletedMessages2.size());
    }

    @Test
    public void removeNonExistentMessage() throws Exception {
        Id one = instance.add(new BytesMessage(".".getBytes()));
        Id two = instance.add(new BytesMessage("..".getBytes()));

        Tag a = new Tag("a");
        Tag b = new Tag("b");
        Tag c = new Tag("c");

        instance.tag(new Query(one), a);
        instance.tag(new Query(one), b);
        instance.tag(new Query(one), c);
        instance.tag(new Query(two), a);
        instance.tag(new Query(two), c);

        Set<PersistentMessage> deletedMessages1 = instance.remove(Queries.fromTags("a", "b"));
        assertEquals((long) one.value(), first(deletedMessages1).getId());
        assertEquals(1, deletedMessages1.size());

        Set<PersistentMessage> deletedMessages2 = instance.remove(Queries.fromTags("a", "c"));
        assertEquals((long) two.value(), first(deletedMessages2).getId());
        assertEquals(1, deletedMessages2.size());

        Set<PersistentMessage> deletedMessages3 = instance.remove(Queries.fromTags("c"));
        assertEquals(0, deletedMessages3.size());
    }

    @Test
    public void removeMessagesWithTagsAndTimestamp() throws Exception {
        Id one = instance.add(new BytesMessage(".".getBytes()));
        Id two = instance.add(new BytesMessage("..".getBytes()));
        Id three = instance.add(new BytesMessage("..".getBytes()));

        Tag a = new Tag("a");
        Tag b = new Tag("b");
        Tag c = new Tag("c");

        instance.tag(new Query(one), a);
        instance.tag(new Query(one), b);
        instance.tag(new Query(two), a);
        instance.tag(new Query(two), b);
        instance.tag(new Query(three), c);
        
        Query query1 = new Query(a,b,new com.intel.icecp.module.query.Before(1));

        Set<PersistentMessage> messagesBefore = instance.remove(query1);
        assertEquals(0, messagesBefore.size());

        Thread.sleep(1000);

        Set<PersistentMessage> messagesAfter = instance.remove(query1);
        assertEquals(2, messagesAfter.size());
        
        Thread.sleep(1000);
        
        //Message with this query already removed by query1.   
        Query query2 = new Query(a,b,new com.intel.icecp.module.query.Before(2));
        Set<PersistentMessage> messagesAfter2 = instance.remove(query2);
        assertEquals(0, messagesAfter2.size());
        
        Query query3 = new Query(c,new com.intel.icecp.module.query.Before(2));
        Set<PersistentMessage> messagesAfter3 = instance.remove(query3);
        assertEquals((long) three.value(), first(messagesAfter3).getId());
        assertEquals(1, messagesAfter3.size());

    }

    private <T> T first(Iterable<T> iterable) {
        return iterable.iterator().next();
    }

    private boolean contains(Set<PersistentMessage> messages, String s) {
        return messages.stream().anyMatch(m -> Arrays.equals(m.getMessageContent(), s.getBytes()));
    }

    private void addAndTagSomeMessages(int count, String... tags) throws Exception {
        LOGGER.info("Adding {} messages with tags {}", count, tags);
        for (int i = 1; i <= count; i++) {
            byte[] content = Integer.toString(i).getBytes();
            Id created = instance.add(new BytesMessage(content));

            for (String t : tags) {
                instance.tag(new Query(created), new Tag(t));
            }
        }
    }
}