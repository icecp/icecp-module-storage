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

package com.intel.icecp.module.storage.messages;

/**
 * Type of command messages supported by the storage module
 */
public enum MessageType {
    /**
     * Tag the identified messages with the provided tag name.
     */
    TAG,
    /**
     * Untag the identified messages containing the provided tag name.
     */
    UNTAG,
    /**
     * Deletes tagged messages
     */
    DELETE_BY_TAG,
    /**
     * Get the time range of active messages under a specified session channel
     */
    GET_TIME_SPAN,
    /**
     * Deletes all message in a storage session
     */
    DELETE_SESSION,
    /**
     * Deletes all messages in a specified range that exist in the same batch
     */
    DELETE_MESSAGE_BY_RANGE,
    /**
     * Starts a new session for storage
     */
    START,
    /**
     * Retrieves a set of messages for a storage session
     */
    GET,
    /**
     * Stops a storage session from recording
     */
    STOP,
    /**
     * Create a new session from an existing session
     */
    RENAME,
    /**
     * Find all sessions based on a channel
     */
    QUERY,
    /**
     * Retrieves number of messages in a session
     */
    SIZE
}
