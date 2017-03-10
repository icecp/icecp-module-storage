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

package com.intel.icecp.module.query;

/**
 * Represent the set of messages timestamped before the given relative time (in seconds). For example, if the value is
 * 60, this instance would be used to retrieve all messages timestamped 60 seconds or before the current time when the
 * query is executed.
 *
 */
public final class Before implements Query.Identifier<Long> {
    private final long value;

    /**
     * @param seconds the relative number of seconds before the time the query is executed
     */
    public Before(long seconds) {
        this.value = seconds;
    }

    @Override
    public Long value() {
        return value;
    }
}
