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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * For proving out OrientDB transaction concepts
 *
 */
@Ignore
public class OrientDBConnectionTest {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final String URI = "plocal:./db";
    private OrientGraph db;

    @Before
    public void before() {
        db = OrientDBConnection.start(URI);
    }

    @After
    public void after() {
        if (!db.isClosed())
            OrientDBConnection.drop(db);
    }

    @Test
    public void createDatabase() {
        assertNotNull(db);
    }

    @Test
    public void isAlwaysStartingEmpty() {
        assertEquals(0, OrientDBConnection.count(db));
    }

    @Test
    public void createAndStopDatabase() {
        OrientDBConnection.stop(db);
        assertTrue(db.isClosed());
    }

    // WARNING: inconsistent for users
    @Test
    public void disableAutoTransactionsAndManuallyBegin() {
        db.setAutoStartTx(false);

        OrientDBConnection.beginTransaction(db);
        OrientDBConnection.add(db, ".");
        OrientDBConnection.commitTransaction(db);
        assertEquals(0, OrientDBConnection.count(db)); // makes no sense; data should be here at this point

        restart(); // this must commit to the database?
        assertEquals(1, OrientDBConnection.count(db));
    }

    // WARNING: inconsistent for users
    @Test
    public void disableAutoTransactionsAndManuallyCommitTwice() {
        db.setAutoStartTx(false);

        OrientDBConnection.beginTransaction(db);
        OrientDBConnection.add(db, ".");
        OrientDBConnection.commitTransaction(db);
        OrientDBConnection.commitTransaction(db); // THE JIM HACK
        assertEquals(1, OrientDBConnection.count(db));

        restart(); // this must commit to the database?
        assertEquals(1, OrientDBConnection.count(db));
    }

    // WARNING: inconsistent for users
    @Test
    public void autoTransactionsAreDifferentAfterRestart() {
        db.setAutoStartTx(true); // unnecessary line, is true by default; kept for clarity

        OrientDBConnection.add(db, ".");
        assertEquals(0, OrientDBConnection.count(db)); // makes no sense, eventually committed by shutdown/close?

        restart();
        assertEquals(1, OrientDBConnection.count(db));
    }

    @Test
    public void autoTransactionsWithManualCommit() {
        db.setAutoStartTx(true); // unnecessary line, is true by default; kept for clarity

        OrientDBConnection.add(db, ".");
        OrientDBConnection.commitTransaction(db);
        assertEquals(1, OrientDBConnection.count(db));

        restart();
        assertEquals(1, OrientDBConnection.count(db));
    }

    @Test
    public void autoTransactionsWithManualCommitWithMultipleItems() {
        OrientDBConnection.add(db, ".");
        OrientDBConnection.add(db, "..");
        OrientDBConnection.add(db, "...");
        OrientDBConnection.commitTransaction(db); // if this is not done,
        assertEquals(3, OrientDBConnection.count(db));

        restart();
        assertEquals(3, OrientDBConnection.count(db));
    }

    @Test
    public void rollbackShouldResultInNoChange() {
        db.setAutoStartTx(false);

        try {
            OrientDBConnection.beginTransaction(db); // this could contain the setAutoStartTx to true
            OrientDBConnection.add(db, ".");
            OrientDBConnection.add(db, "..");
            throwException();
            OrientDBConnection.add(db, "...");
            OrientDBConnection.commitTransaction(db); // ... and reset setAutoStartTx back to true
        } catch (IOException e) {
            OrientDBConnection.rollbackTransaction(db);
        }
        assertEquals(0, OrientDBConnection.count(db));

        restart();
        assertEquals(0, OrientDBConnection.count(db));
    }

    @Test
    public void threadCompetition() throws InterruptedException {
        final int NUM_THREADS = 16;
        final int NUM_VERTICES = 100;

        // setup
        db.setAutoStartTx(false);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);
        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        Runnable addMultipleVertices = () -> {
            for (int i = 0; i < NUM_VERTICES; i++) {
                OrientDBConnection.beginTransaction(db); // semaphore += 2
                OrientDBConnection.add(db, i);
                OrientDBConnection.commitTransaction(db); // semaphore -= 1
                OrientDBConnection.commitTransaction(db); // THE JIM HACK; semaphore -= 1
            }

            LOGGER.info("{} complete", Thread.currentThread());
            latch.countDown();
        };

        // submit task to all threads; TODO they could all wait to start at the same time but don't
        for (int i = 0; i < NUM_THREADS; i++) {
            pool.submit(addMultipleVertices);
        }

        assertTrue("all threads have completed", latch.await(15, TimeUnit.SECONDS));
        assertEquals("the expected number of vertices are created", NUM_THREADS * NUM_VERTICES, OrientDBConnection.count(db));
    }

    private void throwException() throws IOException {
        throw new IOException("Generated by test");
    }

    private void restart() {
        OrientDBConnection.stop(db);
        db = OrientDBConnection.start(URI);
    }
}