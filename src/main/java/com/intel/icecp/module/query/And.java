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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Represent a logical-AND conjunction of identifiers (tags or message IDs) that can be used for selecting messages
 * in the database. Note: currently this implementation is limited to one level and one list of identifiers. In cases
 * where a message ID is used, this operator only makes sense when it contains one {@link Id} (if it contained more than
 * one, nothing would be selected).
 *
 */
public class And implements Query.Operator {
    private final Set<Query.Element> children = new LinkedHashSet<>(); // used to maintain order of elements added

    public And(Query.Identifier... ids) {
        this.children.addAll(Arrays.asList(ids));
    }

    @Override
    public Set<Query.Element> children() {
        return Collections.unmodifiableSet(children);
    }
}
