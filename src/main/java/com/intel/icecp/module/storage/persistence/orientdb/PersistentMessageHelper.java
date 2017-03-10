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

import com.intel.icecp.module.storage.persistence.PersistentMessage;
import com.tinkerpop.blueprints.Vertex;

/**
 * For converting vertices to their module-specific class
 *
 */
final class PersistentMessageHelper {

    private PersistentMessageHelper() {
        // do not allow instances of this class
    }

    static PersistentMessage fromVertex(Vertex persistentMessageVertex) {
        PersistentMessage pm = null;
        if (persistentMessageVertex != null) {
            pm = new PersistentMessage(
                    persistentMessageVertex.getProperty(OrientDbNamespace.MESSAGE_ID_PROPERTY),
                    persistentMessageVertex.getProperty(OrientDbNamespace.MESSAGE_TIMESTAMP_PROPERTY),
                    persistentMessageVertex.getProperty(OrientDbNamespace.MESSAGE_CONTENT_PROPERTY));
        }
        return pm;
    }
}
