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

import com.intel.icecp.module.query.And;
import com.intel.icecp.module.query.Before;
import com.intel.icecp.module.query.Id;
import com.intel.icecp.module.query.Query;
import com.intel.icecp.module.query.Tag;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Adapt an {@link And} operator to the OrientDB database; this class is responsible for querying the database for the
 * given selectors and allowing iteration over the results. Remember that selectors for these queries will be logically
 * ANDed; e.g. if tags "a" and "b" are passed in {@link And}, the results will be only of vertices that are tagged with
 * "a" AND "b".
 *
 */
class OsqlAnd implements Iterable<OIdentifiable> {
    private static final Logger LOGGER = LogManager.getLogger();
    private final Set<Query.Identifier> selectors;
    private OrientGraph db;

    OsqlAnd(OrientGraph db, And and) {
        this.db = db;
        this.selectors = (Set) and.children(); // note that we limit this AND to only contain identifiers as a temporary measure until multi-level can be implemented
    }

    static Object[] concatenate(QueryPair... pairs) {
        return concatenate(Arrays.stream(pairs).map(q -> q.params).toArray(Object[][]::new));
    }

    static Object[] concatenate(Object[]... params) {
        int count = Arrays.stream(params).mapToInt(a -> a.length).sum();
        ArrayList<Object> objects = new ArrayList<>(count);
        Arrays.stream(params).forEach(a -> objects.addAll(Arrays.asList(a)));
        return objects.toArray(new Object[count]);
    }

    private static QueryPair toQuery(Query.Identifier selector) {
        if (selector instanceof Tag) {
            return toQuery((Tag) selector);
        } else if (selector instanceof Id) {
            return toQuery((Id) selector);
        } else if (selector instanceof Before) {
            return toQuery((Before) selector);
        } else {
            throw new IllegalArgumentException("Unknown identifier type passed; only ID and TAG are currently supported: " + selector);
        }
    }

    private static QueryPair toQuery(Tag tag) {
        String osql = "(SELECT expand(in()) FROM " + OrientDbNamespace.TAG_CLASS + " WHERE " + OrientDbNamespace.TAG_NAME_PROPERTY + " = ?)"; // TODO limit this to only vertices of the Message class?
        return new QueryPair(osql, tag.value());
    }

    private static QueryPair toQuery(Id id) {
        String osql = "(SELECT expand(@rid) FROM " + OrientDbNamespace.MESSAGE_CLASS + " WHERE " + OrientDbNamespace.MESSAGE_ID_PROPERTY + " = ?)";
        return new QueryPair(osql, id.value());
    }

    private static QueryPair toQuery(Before before) {
        return toQuery(before, System.currentTimeMillis());
    }

    static QueryPair toQuery(Before before, long currentTimeMs) {
        long beforeNowMs = Math.multiplyExact(Math.max(0, before.value()), 1000); // handle values below 0 and overflows
        long absoluteBeforeMs = currentTimeMs - beforeNowMs;
        String osql = "(SELECT expand(@rid) FROM " + OrientDbNamespace.MESSAGE_CLASS + " WHERE " + OrientDbNamespace.MESSAGE_TIMESTAMP_PROPERTY + " <= ?)";
        return new QueryPair(osql, absoluteBeforeMs);
    }

    private static <T> T first(Iterable<T> iterable) {
        Iterator<T> it = iterable.iterator();
        if (it.hasNext()) {
            return it.next();
        }
        throw new NoSuchElementException("Failed to find first element!");
    }

    /**
     * @return a query pair consisting of the OSQL and list of bound parameters to filter the result set
     */
    QueryPair toQuery() {
        if (selectors.isEmpty()) {
            return null;
        } else if (selectors.size() == 1) {
            QueryPair firstPair = toQuery(first(selectors));
            // see documentation at http://orientdb.com/docs/2.0/orientdb.wiki/SQL-Where.html#record-attributes
            String osql = "SELECT FROM " + firstPair.osql;
            return new QueryPair(osql, firstPair.params);
        } else {
            QueryPair[] qs = selectors.stream().map(OsqlAnd::toQuery).toArray(QueryPair[]::new);
            String queriesOsql = String.join(", ", Arrays.stream(qs).map(q -> q.osql).toArray(CharSequence[]::new));
            // see documentation at http://orientdb.com/docs/2.1/SQL-Functions.html#intersect
            String osql = "SELECT expand(intersect(" + queriesOsql + "))";
            Object[] params = concatenate(qs);
            return new QueryPair(osql, params);
        }
    }

    @Override
    public Iterator<OIdentifiable> iterator() {
        if (selectors.isEmpty()) {
            LOGGER.debug("No selectors defined for AND query, returning empty iterator");
            return Collections.emptyIterator();
        } else {
            QueryPair q = toQuery();
            LOGGER.debug("Executing AND query '{}' with params {}", q.osql, q.params);
            Iterable<OIdentifiable> executed = db.command(new OCommandSQL(q.osql)).execute(q.params);
            return executed.iterator();
        }
    }

    static class QueryPair {
        String osql;
        Object[] params;

        QueryPair(String osql, Object... params) {
            this.osql = osql;
            this.params = params;
        }
    }
}
