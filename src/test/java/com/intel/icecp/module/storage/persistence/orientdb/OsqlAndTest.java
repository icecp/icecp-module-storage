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