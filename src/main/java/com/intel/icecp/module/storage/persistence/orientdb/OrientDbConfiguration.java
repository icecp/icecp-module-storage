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

/**
 * Configuration class for {@link StorageProviderFacade}
 */
public class OrientDbConfiguration {
    private OrientDbStorageType storageType = OrientDbStorageType.EMBEDDED_GRAPH;
    private String dbFilePath = "/tmp/";
    private String dbFileName = "data";
    private String dbUrlDelimiter = ":";

    /**
     * Gets storage type
     *
     * @return the storage type
     */
    public OrientDbStorageType getStorageType() {
        return storageType;
    }

    /**
     * Sets engine type.
     *
     * @param storageType the engine type
     */
    public void setStorageType(OrientDbStorageType storageType) {
        if (storageType == null) {
            throw new IllegalArgumentException("storageType must not be null!");
        }
        this.storageType = storageType;
    }

    /**
     * Gets db file path.
     * <p>
     * This is only relevant when the {@link OrientDbStorageType}
     * is EMBEDDED_GRAPH
     *
     * @return the db file path
     */
    public String getDbFilePath() {
        return dbFilePath;
    }

    /**
     * Sets db file path.
     * <p>
     * This is only relevant when the {@link OrientDbStorageType}
     * is EMBEDDED_GRAPH
     *
     * @param dbFilePath the db file path
     */
    public void setDbFilePath(String dbFilePath) {
        if (dbFilePath == null || dbFilePath.trim().isEmpty()) {
            throw new IllegalArgumentException("dbFilePath must not be null or blank!");
        }
        this.dbFilePath = dbFilePath;
    }

    /**
     * Gets db file name.
     *
     * @return the db file name
     */
    public String getDbFileName() {
        return dbFileName;
    }

    /**
     * Sets db file name.
     *
     * @param dbFileName the db file name
     */
    public void setDbFileName(String dbFileName) {
        if (dbFileName == null || dbFileName.trim().isEmpty()) {
            throw new IllegalArgumentException("dbFileName must not be null or blank!");
        }
        this.dbFileName = dbFileName;
    }

    /**
     * Gets db url delimiter.
     *
     * @return the db url delimiter
     */
    public String getDbUrlDelimiter() {
        return dbUrlDelimiter;
    }

    /**
     * Sets db url delimiter.
     *
     * @param dbUrlDelimiter the db url delimiter
     */
    public void setDbUrlDelimiter(String dbUrlDelimiter) {
        if (dbUrlDelimiter == null || dbUrlDelimiter.trim().isEmpty()) {
            throw new IllegalArgumentException("dbUrlDelimiter must not be null or blank!");
        }
        this.dbUrlDelimiter = dbUrlDelimiter;
    }
}
