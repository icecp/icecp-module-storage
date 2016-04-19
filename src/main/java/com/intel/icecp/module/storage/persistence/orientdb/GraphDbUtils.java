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
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * OrientDb Graph database instance / operational utilities.
 *
 */
public final class GraphDbUtils {
    private static final Logger LOGGER = LogManager.getLogger(GraphDbUtils.class.getName());
    private static final int MIN_POOL_SIZE = 1;
    private static final int MAX_POOL_SIZE = 500;

    private GraphDbUtils() {
    }

    /**
     * Get an OrientDB graph database instance based on the configuration,
     * {@link OrientDbConfiguration} <code> configuration</code>.
     *
     * @param configuration the OrientDB configuration
     * @return an OrientDB graph database instance
     * @see OrientDbConfiguration
     */
    public static OrientGraph getGraphDbInstance(OrientDbConfiguration configuration) {
        if (configuration == null) {
            LOGGER.error("Configuration is null!");
            return null;
        }

        String dbUrl = getDbURL(configuration);
        OrientGraph graphDb = null;
        if (dbUrl != null) {
            // setup database connection pooling:
            OrientGraphFactory factory = new OrientGraphFactory(dbUrl).setupPool(MIN_POOL_SIZE, MAX_POOL_SIZE);
            // create an instance using default
            graphDb = factory.getTx();
        }
        return graphDb;
    }

    /**
     * Close an OrientDB graph database instance <code> graphDb</code>. Once the
     * instance is close, the instance, <code>graphDb</code> cannot be re-used
     * again.
     *
     * @param graphDb the OrientDB graph database instance to be closed down.
     */
    public static void shutdownDbInstance(OrientGraph graphDb) {
        if (graphDb != null) {
            graphDb.getRawGraph().close();
            graphDb.shutdown();
        }
    }

    /**
     * drop the OrientDB graph database instance <code> graphDb</code>.
     *
     * @param graphDb the OrientDB graph database instance to be closed down.
     */
    public static void dropDbInstance(OrientGraph graphDb) {
        if (graphDb != null) {
            graphDb.drop();
        }
    }

    /**
     * Returns a collection {@link List} of the same type from an
     * {@link Iterable} of type {@code O}. If the instance {@code iterable} is
     * already a List type, then this instance itself gets returned.
     *
     * @param iterable an iterable instance.
     * @param <O> the data type.
     * @return an ArrayList collection.
     */
    public static <O> List<O> asList(Iterable<O> iterable) {
        if (iterable instanceof List) {
            return (List<O>) iterable;
        }

        List<O> list = new ArrayList<>();
        if (iterable != null) {
            for (O obj : iterable) {
                list.add(obj);
            }
        }
        return list;
    }

    private static String getDbURL(OrientDbConfiguration configuration) {
        StringBuilder dbUrlBuf = new StringBuilder();
        if (OrientDbStorageType.EMBEDDED_GRAPH.equals(configuration.getStorageType())) {
            dbUrlBuf.append(configuration.getStorageType())
                    .append(configuration.getDbUrlDelimiter())
                    .append(configuration.getDbFilePath())
                    .append(configuration.getDbFileName());
        } else if (OrientDbStorageType.IN_MEMORY_GRAPH.equals(configuration.getStorageType())) {
            dbUrlBuf.append(configuration.getStorageType())
                    .append(configuration.getDbUrlDelimiter())
                    .append(configuration.getDbFileName());
        } else {
            LOGGER.error("Unsupported graph db engine type!");
        }
        return dbUrlBuf.toString();
    }
}
