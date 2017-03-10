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
