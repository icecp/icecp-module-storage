/*
 * ******************************************************************************
 *
 *  INTEL CONFIDENTIAL
 *
 *  Copyright 2013 - 2016 Intel Corporation All Rights Reserved.
 *
 *  The source code contained or described herein and all documents related to the
 *  source code ("Material") are owned by Intel Corporation or its suppliers or
 *  licensors. Title to the Material remains with Intel Corporation or its
 *  suppliers and licensors. The Material contains trade secrets and proprietary
 *  and confidential information of Intel or its suppliers and licensors. The
 *  Material is protected by worldwide copyright and trade secret laws and treaty
 *  provisions. No part of the Material may be used, copied, reproduced, modified,
 *  published, uploaded, posted, transmitted, distributed, or disclosed in any way
 *  without Intel's prior express written permission.
 *
 *  No license under any patent, copyright, trade secret or other intellectual
 *  property right is granted to or conferred upon you by disclosure or delivery of
 *  the Materials, either expressly, by implication, inducement, estoppel or
 *  otherwise. Any license under such intellectual property rights must be express
 *  and approved by Intel in writing.
 *
 *  Unless otherwise agreed by Intel in writing, you may not remove or alter this
 *  notice or any other notice embedded in Materials by Intel or Intel's suppliers
 *  or licensors in any way.
 *
 * *********************************************************************
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
