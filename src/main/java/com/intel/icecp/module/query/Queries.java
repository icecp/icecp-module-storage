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

package com.intel.icecp.module.query;

import com.intel.icecp.module.storage.persistence.providers.TaggedStorageProvider;

import java.util.Arrays;

/**
 * Factory methods for building queries for common case selections
 *
 */
public class Queries {

    private Queries() {
        // do not instantiate this class
    }

    /**
     * @param id the message ID to query for
     * @return a query to be passed to {@link TaggedStorageProvider}
     */
    public static Query fromId(long id) {
        return new Query(new Id(id));
    }

    /**
     * @param tags the logically-ANDed tags to group messages
     * @return a query to be passed to {@link TaggedStorageProvider}
     */
    public static Query fromTags(String... tags) {
        return new Query(Arrays.stream(tags).map(Tag::new).toArray(Query.Identifier[]::new));
    }
}
