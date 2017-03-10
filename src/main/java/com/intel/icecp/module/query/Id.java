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
 * Represent a stored message's database ID; this can be used for querying for a message.
 *
 */
public final class Id implements Query.Identifier<Long> {
    private final long value;

    public Id(long id) {
        this.value = id;
    }

    @Override
    public Long value() {
        return value;
    }
}
