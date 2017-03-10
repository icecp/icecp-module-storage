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

import com.intel.icecp.core.messages.BytesMessage;
import com.intel.icecp.module.query.Id;
import com.intel.icecp.module.query.Query;
import com.intel.icecp.module.query.Tag;
import com.intel.icecp.module.storage.exceptions.TaggingOperationException;
import com.intel.icecp.module.storage.persistence.PersistentMessage;

import java.util.Set;

/**
 * Exposed the API for tag-related storage activities; some of the previously-implemented methods in {@link
 * LegacyStorageProvider} are now unnecessary (rename, delete-by-range, previous-session, etc.) and will eventually be
 * removed. However, these APIs should for the time being remain in place together until they can be merged.
 * <p>
 * The implementation we discussed would create OrientDB "classes" for each tag added to a message; the rationale for
 * this would be to allow for indexed queries using the tags (as in the case of {@link
 * QueriesStorageProvider#getActiveMaximumTimestamp(String)}.
 * <p>
 * In pseudo-code, this API might be used like:
 * <p>
 * <pre><code>
 * // add a message
 * var id = add(...)
 * tag(id, "message"), class = "PersistentMessage"
 * tag(id, "active"), class = "Active"
 *
 * // tag it with its channel name and then tag the channel name as a channel
 * var cid = tag(id, "ndn:/a/b/c")
 * tag(cid, "channel")
 *
 * // tag it with its session ID and then tag this as a session
 * var sid = tag(id, "sid:42")
 * tag(sid, "session")
 *
 * // inactivate some messages
 * var toRemove = "active"
 * untag(toRemove, "sid:42", "ndn:/a/b/c")
 *
 * // query the DB for all messages tagged
 * find("active", "ndn:/a/b/c")
 * </code></pre>
 */
public interface TaggedStorageProvider extends QueriesStorageProvider {

    /**
     * @param message the message to add
     * @return the new, unique ID of the added message
     * @throws TaggingOperationException if the operation fails; if thrown, the message has not been added
     */
    Id add(BytesMessage message) throws TaggingOperationException;

    /**
     * @param query selects the messages to be returned
     * @return the messages that have all of the passed tags
     * @throws TaggingOperationException if the operation fails
     */
    Set<PersistentMessage> find(Query query) throws TaggingOperationException;

    /**
     * @param query selects the messages to be removed
     * @return the removed messages
     * @throws TaggingOperationException if the operation fails; if thrown, no items selected by the query will be
     * removed
     */
    Set<PersistentMessage> remove(Query query) throws TaggingOperationException;

    /**
     * @param query selects the messages to be tagged
     * @param tag the tag to mark messages with
     * @return the ID of the tag's vertex (may be created if it doesn't exist)
     * @throws TaggingOperationException if the operation fails; if thrown, no items selected by the query will be
     * tagged
     */
    Set<Id> tag(Query query, Tag tag) throws TaggingOperationException;

    /**
     * @param query selects the messages to be untagged
     * @param tag the tag to remove from the message
     * @return the ID of the tag's vertex (may be removed if it has no more edges)
     * @throws TaggingOperationException if the operation fails; if thrown, no items selected by the query will be
     * untagged
     */
    Set<Id> untag(Query query, Tag tag) throws TaggingOperationException;

    /**
     * @param tags the matching tags to use for finding the related tags; e.g. if a set of messages is tagged with
     * channel "/a/b/c", calling {@code related("/a/b/c")} may return {"message", "active", "sid:1", "sid:2", "/a/b/c"}.
     * Note that the parameters themselves should be part of the returned set.
     * @return the set of tags that also are linked to the vertex set given by the tags parameter
     * @throws TaggingOperationException if the operation fails
     */
    Set<Tag> related(Tag... tags) throws TaggingOperationException;
}
