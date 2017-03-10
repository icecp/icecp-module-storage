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

import org.junit.Before;
import org.junit.Test;

public class OrientDbConfigurationTest {
    private OrientDbConfiguration orientDbConfiguration;

    @Before
    public void setup() {
        orientDbConfiguration = new OrientDbConfiguration();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsExceptionWhenEngineTypeIsNull() throws Exception {
        orientDbConfiguration.setStorageType(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setDbFilePathThrowsExceptionWhenPathIsNull() throws Exception {
        orientDbConfiguration.setDbFilePath(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setDbFilePathThrowsExceptionWhenPathIsEmpty() throws Exception {
        orientDbConfiguration.setDbFilePath(" ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setDbFileNameThrowsExceptionWhenNameIsNull() throws Exception {
        orientDbConfiguration.setDbFileName(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setDbFileNameThrowsExceptionWhenNameIsEmpty() throws Exception {
        orientDbConfiguration.setDbFileName(" ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setDbUrlDelimiterThrowsExceptionWhenNameIsNull() throws Exception {
        orientDbConfiguration.setDbUrlDelimiter(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setDbUrlDelimiterThrowsExceptionWhenNameIsEmpty() throws Exception {
        orientDbConfiguration.setDbUrlDelimiter(" ");
    }
}
