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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Represent a logical-AND conjunction of identifiers (tags or message IDs) that can be used for selecting messages
 * in the database. Note: currently this implementation is limited to one level and one list of identifiers. In cases
 * where a message ID is used, this operator only makes sense when it contains one {@link Id} (if it contained more than
 * one, nothing would be selected).
 *
 */
public class And implements Query.Operator {
    private final Set<Query.Element> children = new LinkedHashSet<>(); // used to maintain order of elements added

    public And(Query.Identifier... ids) {
        this.children.addAll(Arrays.asList(ids));
    }

    @Override
    public Set<Query.Element> children() {
        return Collections.unmodifiableSet(children);
    }
}
