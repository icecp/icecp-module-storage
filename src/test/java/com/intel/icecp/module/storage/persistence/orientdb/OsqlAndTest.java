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
import com.intel.icecp.module.query.Id;
import com.intel.icecp.module.query.Tag;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 */
public class OsqlAndTest {

    private OrientGraph db;

    @Before
    public void before() {
        db = mock(OrientGraph.class);
    }

    @Test
    public void concatenateObjects() {
        Object[] concatenated = OsqlAnd.concatenate(new Object[]{"a", "b"}, new Object[]{"c"});

        assertEquals(3, concatenated.length);
        assertArrayEquals(new Object[]{"a", "b", "c"}, concatenated);
    }

    @Test
    public void concatenateQueries() {
        OsqlAnd.QueryPair first = new OsqlAnd.QueryPair("SELECT FROM ...", "a", "b");
        OsqlAnd.QueryPair second = new OsqlAnd.QueryPair("SELECT FROM ...", "c");

        Object[] concatenated = OsqlAnd.concatenate(first, second);

        assertEquals(3, concatenated.length);
        assertArrayEquals(new Object[]{"a", "b", "c"}, concatenated);
    }

    @Test(expected = NoSuchElementException.class)
    public void noSelectors() {
        And and = new And();
        OsqlAnd instance = new OsqlAnd(db, and);

        assertFalse(instance.iterator().hasNext());
        instance.iterator().next();
    }

    @Test
    public void toQueryWithOneSelector() {
        OsqlAnd instance = new OsqlAnd(db, new And(new Id(42)));

        OsqlAnd.QueryPair q = instance.toQuery();

        assertEquals(1, q.params.length);
        assertEquals(42L, q.params[0]);
        assertEquals(2, timesContained("SELECT", q.osql));
    }

    @Test
    public void toQueryWithMultipleSelectors() {
        OsqlAnd instance = new OsqlAnd(db, new And(new Tag("a"), new Tag("b"), new com.intel.icecp.module.query.Before(60)));

        OsqlAnd.QueryPair q = instance.toQuery();

        assertEquals(3, q.params.length);
        assertEquals("a", q.params[0]);
        assertEquals("b", q.params[1]);
        assertTrue((long) q.params[2] > 0); // cannot do assertArrayEquals because the relative before time is converted to an absolute time
        assertEquals(4, timesContained("SELECT", q.osql));
    }

    @Test
    public void handleNegativeBefore() throws Exception {
        OsqlAnd.QueryPair q = OsqlAnd.toQuery(new com.intel.icecp.module.query.Before(Long.MIN_VALUE), 10);
        assertEquals(10L, q.params[0]);
    }

    @Test(expected = ArithmeticException.class)
    public void handleOverflowingBefore() throws Exception {
        OsqlAnd.toQuery(new com.intel.icecp.module.query.Before(Long.MAX_VALUE), 10);
    }

    private int timesContained(String needle, String haystack) {
        return haystack.split(needle, -1).length - 1;
    }
}