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

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.graph.sql.functions.OGraphFunctionFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * Holds the names of the classes and fields used for interacting with the Orient database. Also used for setting this
 * namespace up on a new database.
 *
 */
final class OrientDbNamespace {
    private static final Logger LOGGER = LogManager.getLogger();

    static final String ID_SEQUENCE = "IDs";

    static final String TAG_CLASS = "Tag";
    static final String TAG_NAME_PROPERTY = "name";
    static final String TAG_NAME_INDEX = "Tag.NameIndex";

    static final String MESSAGE_CLASS = "Message";
    static final String MESSAGE_ID_PROPERTY = "mid";
    static final String MESSAGE_TIMESTAMP_PROPERTY = "ts";
    static final String MESSAGE_CONTENT_PROPERTY = "d";
    static final String MESSAGE_TAG_RELATIONSHIP = "tagged-by";

    static final String INACTIVE_TAG = "inactive"; // TODO remove if possible, necessary for retrieving only active messages from the legacy storage provider

    // from legacy OrientDB provider:
    static final String SESSION_CLASS = "session";
    static final String SESSION_VERTEX_CLASS_NAME = "class:" + SESSION_CLASS;
    static final String SESSION_ID_KEY = "sessionId";
    static final String SESSION_ID_VERTEX_KEY = SESSION_CLASS + "." + SESSION_ID_KEY;
    static final String SESSION_CHANNEL_KEY = "channelName";
    static final String SESSION_NEXT_INDEX_KEY = "nextIndex";
    static final String SESSION_MAX_BUFFER_PERIOD_IN_SEC_KEY = "maxBufferPeriodInSec";
    static final String SESSION_CHANNEL_VERTEX_KEY = SESSION_CLASS + "." + SESSION_CHANNEL_KEY;
    static final String SESSION_SESSION_RELATIONSHIP = "sessionLinks";
    static final String SESSION_MESSAGE_RELATIONSHIP = "collects";
    static final String SESSION_MESSAGE_RELATIONSHIP_INDEX = "index";

    private OrientDbNamespace() {
        // do not allow instances of this class
    }

    /**
     * Add all necessary schema things like classes, sequences, indices to a graph
     *
     * @param graph the Orient database instance
     */
    static synchronized void setupSchemata(OrientGraph graph) {
        if (graph == null)
            throw new IllegalArgumentException("Orient graph is null!");

        // disable auto start transaction by default
        graph.setAutoStartTx(false);

        graph.begin();

        // classes:
        addOrUpdateClass(graph, TAG_CLASS, GraphClassType.VERTEX);
        addOrUpdateClass(graph, MESSAGE_CLASS, GraphClassType.VERTEX);
        addOrUpdateClass(graph, SESSION_CLASS, GraphClassType.VERTEX);

        addOrUpdateClass(graph, MESSAGE_TAG_RELATIONSHIP, GraphClassType.EDGE);
        addOrUpdateClass(graph, SESSION_SESSION_RELATIONSHIP, GraphClassType.EDGE);
        addOrUpdateClass(graph, SESSION_MESSAGE_RELATIONSHIP, GraphClassType.EDGE);

        // unique sequences:
        addOrUpdateSequence(graph, ID_SEQUENCE);

        // indices:
        addTagIndex(graph);

        graph.commit();

        registerOrientDbGraphFunctions();
    }

    private static void registerOrientDbGraphFunctions() {
        OGraphFunctionFactory graphFunctionFactory = new OGraphFunctionFactory();
        Set<String> graphFunctionNames = graphFunctionFactory.getFunctionNames();
        for (String graphFuncName : graphFunctionNames) {
            try {
                OSQLEngine.getInstance().getFunction(graphFuncName);
            } catch (OCommandSQLParsingException e) {
                LOGGER.trace("OSQLEngine failed to get function {} {}", graphFuncName, e);
                // register the missing function for OSQLEngine:
                OSQLEngine.getInstance().registerFunction(graphFuncName, graphFunctionFactory.createFunction(graphFuncName));
                LOGGER.info("Register OrientDb graph function {} for OSQL {}", graphFuncName, OSQLEngine.getInstance().getFunction(graphFuncName).getName());
            }
        }
    }

    private static void addTagIndex(OrientGraph graph) {
        if (graph.getRawGraph().getMetadata().getSchema().getClass(TAG_CLASS).getClassIndex(TAG_NAME_INDEX) == null) {
            // if executed within a transaction, OrientGraph logs warnings, therefore...
            graph.executeOutsideTx(g -> {
                OSchemaProxy schema = g.getRawGraph().getMetadata().getSchema();
                OClass tagClass = schema.getOrCreateClass(TAG_CLASS);

                // see documentation at http://orientdb.com/docs/2.1/Console-Command-Create-Index.html
                tagClass.createProperty(TAG_NAME_PROPERTY, OType.STRING);
                tagClass.createIndex(TAG_NAME_INDEX, OClass.INDEX_TYPE.UNIQUE, TAG_NAME_PROPERTY);
                schema.save();
                return null;
            });

        }
    }

    private static void addOrUpdateClass(OrientGraph graph, @NotNull String className, @NotNull GraphClassType graphClassType) {
        if (graph.getRawGraph().getMetadata().getSchema().getClass(className) == null) {
            // if executed within a transaction, OrientGraph logs warnings, therefore...
            graph.executeOutsideTx(g -> {
                switch (graphClassType) {
                    case VERTEX:
                        g.createVertexType(className);
                        break;
                    case EDGE:
                        g.createEdgeType(className);
                        break;
                    default:
                        // not supported: do nothing...
                }
                return null;
            });
        }
    }

    private static void addOrUpdateSequence(OrientGraph graph, String sequenceName) {
        OSequenceLibrary sequenceLibrary = graph.getRawGraph().getMetadata().getSequenceLibrary();
        if (sequenceLibrary.getSequence(sequenceName) == null) {
            // see documentation at http://orientdb.com/docs/2.1/Sequences-and-auto-increment.html
            OSequence.CreateParams params = new OSequence.CreateParams().setDefaults();
            sequenceLibrary.createSequence(sequenceName, OSequence.SEQUENCE_TYPE.ORDERED, params);
        }
    }

    /**
     * Internal enumeration for types of graph classes
     */
    private enum GraphClassType {
        VERTEX, EDGE
    }
}
