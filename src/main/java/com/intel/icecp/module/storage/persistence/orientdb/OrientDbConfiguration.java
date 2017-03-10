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
