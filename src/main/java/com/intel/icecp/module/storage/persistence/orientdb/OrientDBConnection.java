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

import com.intel.icecp.module.storage.exceptions.InconsistentStateException;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Simple DB connector to prove basic functionality
 *
 */
public class OrientDBConnection {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int MIN_POOL_SIZE = 1;
    private static final int MAX_POOL_SIZE = 500;
    private static Semaphore lock = new Semaphore(2);

    private OrientDBConnection() {
        // private constructor to hide the implicit public one
    }
    public static OrientGraph start(String uri) {
        OrientGraphFactory factory = new OrientGraphFactory(uri).setupPool(MIN_POOL_SIZE, MAX_POOL_SIZE);
        OrientGraph db = factory.getTx();
        lock = new Semaphore(2);

        LOGGER.info("Created database: {}", db);
        return db;
    }

    public static void stop(OrientGraph db) {
        lock.release(2);
        db.getRawGraph().close();
        db.shutdown();

        LOGGER.info("Stopped database: {}", db);
    }

    public static void drop(OrientGraph db) {
        db.drop();
        LOGGER.info("Dropped database: {}", db);
    }

    public static void beginTransaction(OrientGraph db) {
        try {
            if (!lock.tryAcquire(2, 60, TimeUnit.SECONDS)) {
                throw new InconsistentStateException("Failed to acquire exclusive lock for the transaction"); // TODO throw StorageModuleException
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (InconsistentStateException e) {
            LOGGER.error("Failed to acquire exclusive lock for the transaction", e);
        }
        // only one-at-a-time past this point

        LOGGER.info("Beginning transaction on database: {}", db);
        db.getRawGraph().activateOnCurrentThread();
        db.begin();
    }

    public static void commitTransaction(OrientGraph db) {
        // TODO not complete
        // only properly started transactions from the thread holding the lock get past this point

        LOGGER.info("Committing transaction on database: {}", db);
        db.getRawGraph().activateOnCurrentThread();
        db.commit();

        lock.release();
    }

    public static void rollbackTransaction(OrientGraph db) {
        LOGGER.info("Rolling back transaction on database: {}", db);
        db.getRawGraph().activateOnCurrentThread();
        db.rollback();

        lock.release(2);
    }

    public static OrientVertex add(OrientGraph db, Object data) {
        return db.addVertex(null,
                "data", data,
                "timestamp", System.currentTimeMillis());
    }

    public static long count(OrientGraph db) {
        String osql = "SELECT COUNT(*) FROM V";
        OCommandSQL cmd = new OCommandSQL(osql);
        Iterable<Vertex> related = db.command(cmd).execute();
        long count = 0L;
        if (related.iterator().hasNext())
            count = related.iterator().next().getProperty("COUNT");

        LOGGER.info("Total vertices counted: {}", count);
        return count;
    }
}
