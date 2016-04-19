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

package com.intel.icecp.module.query;

import java.util.Collections;
import java.util.Set;

/**
 * Represent a complex query to the database; this is used for selecting a set of things on which to execute an
 * operation (e.g. delete). Implementation note: currently this only supports a single-level tree of AND-ed identifiers;
 * in other words, it can only find the intersection of several tags or message IDs.
 *
 */
public final class Query {

    private final Operator root;

    public Query(Identifier... ids) {
        root = new And(ids);
    }

    public Operator root() {
        return root;
    }

    /**
     * The base type for query objects
     */
    @FunctionalInterface
    public interface Element {

        /**
         * @return the child elements of this element
         */
        Set<Element> children();
    }

    /**
     * Models a query element used for identifying a thing or things in the database
     *
     * @param <T> the type of the identifier, e.g. Long, String
     */
    public interface Identifier<T> extends Element {
        @Override
        default Set<Element> children() {
            return Collections.emptySet();
        }

        /**
         * @return the value of the identifier
         */
        T value();
    }

    /**
     * Models a query operator used to
     */
    public interface Operator extends Element {
        @Override
        Set<Element> children();
    }
}
