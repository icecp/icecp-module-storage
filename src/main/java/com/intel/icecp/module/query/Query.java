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

import java.util.Collections;
import java.util.Set;

/**
 * Represent a complex query to the database; this is used for selecting a set of things on which to execute an
 * operation (e.g. delete). Implementation note: currently this only supports a single-level tree of AND-ed identifiers;
 * in other words, it can only find the intersection of several tags or message IDs.
 *
 */
public final class Query {

    private final Operator root;

    public Query(Identifier... ids) {
        root = new And(ids);
    }

    public Operator root() {
        return root;
    }

    /**
     * The base type for query objects
     */
    @FunctionalInterface
    public interface Element {

        /**
         * @return the child elements of this element
         */
        Set<Element> children();
    }

    /**
     * Models a query element used for identifying a thing or things in the database
     *
     * @param <T> the type of the identifier, e.g. Long, String
     */
    public interface Identifier<T> extends Element {
        @Override
        default Set<Element> children() {
            return Collections.emptySet();
        }

        /**
         * @return the value of the identifier
         */
        T value();
    }

    /**
     * Models a query operator used to
     */
    public interface Operator extends Element {
        @Override
        Set<Element> children();
    }
}
