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

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Unit tests for OrientDB graph utilities.
 *
 */
public class GraphDbUtilsTest {

    @Test
    public void testGetGraphDbInstance() {
        OrientDbConfiguration dbConfig = new OrientDbConfiguration();
        OrientGraph graph = null;
        try {
            graph = GraphDbUtils.getGraphDbInstance(dbConfig);
            assertNotNull(graph);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed! ");
        } finally {
            if (graph != null) {
                GraphDbUtils.shutdownDbInstance(graph);
            }
        }

    }

    @Test
    public void testGetGraphDbInstanceWithNullConfig() {
        OrientGraph graph = null;
        try {
            graph = GraphDbUtils.getGraphDbInstance(null);
            assertNull(graph);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed! ");
        } finally {
            if (graph != null) {
                GraphDbUtils.shutdownDbInstance(graph);
            }
        }

    }

    @Test
    public void testAsListWithNormalList() {
        List<Integer> numList = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            numList.add(i);
        }
        try {
            List<Integer> retList = GraphDbUtils.asList(numList);
            assertNotNull(retList);
            assertEquals(numList.size(), retList.size());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed! ");
        }
    }

    @Test
    public void testAsListWithTreeSet() {
        Set<Integer> numSet = new TreeSet<>();
        for (int i = 0; i < 50; i++) {
            numSet.add(i);
        }
        try {
            List<Integer> retList = GraphDbUtils.asList(numSet);
            assertNotNull(retList);
            assertEquals(numSet.size(), retList.size());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed! ");
        }
    }

    @Test
    public void testAsListForEmptyOrNullInputs() {
        // empty
        Iterable<Integer> iterable = new ArrayList<>();
        try {
            List<Integer> retList = GraphDbUtils.asList(iterable);
            assertNotNull(retList);
            assertTrue(retList.isEmpty());

            // null case
            List<Integer> retList2 = GraphDbUtils.asList(null);
            assertNotNull(retList2);
            assertTrue(retList2.isEmpty());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed! ");
        }
    }
}
