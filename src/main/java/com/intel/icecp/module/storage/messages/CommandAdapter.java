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

package com.intel.icecp.module.storage.messages;

import com.intel.icecp.module.storage.StorageModule;
import com.intel.icecp.module.storage.exceptions.StorageModuleException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Wrapper class to invoke command messages. This would be used by a client
 * through the RpcServer call which will have these messages registered as
 * commands.
 * <p>
 * Created by Natalie Gaston, natalie.gaston@intel.com on 5/25/2016.
 */
public class CommandAdapter {
    private static final String REPLAY_CHANNEL_KEY_NAME = "replayChannel";
    private static final String SESSION_ID_KEY_NAME = "sessionId";
    private static final String ONLY_WITH_ACTIVE_MESSAGE_KEY_NAME = "onlyWithActiveMessages";
    private static final String LISTEN_CHANNEL_KEY_NAME = "listenChannel";
    private static final String MAXIMUM_BUFFERING_PERIOD_IN_SEC = "maxBufferingPeriodInSec";
    private static final String QUERY_CHANNEL_KEY_NAME = "queryChannel";
    private static final String QUERY_KEY_NAME = "query";
    private static final String IDS_KEY_NAME = "ids";
    private static final String TAGS_KEY_NAME = "tags";
    private static final String BEFORE_KEY_NAME = "before";
    private static final String LIMIT_KEY_NAME = "limit";
    private static final String SKIP_KEY_NAME = "skip";
    private StorageModule context;

    public CommandAdapter(StorageModule context) {
        this.context = context;
    }

    public Object deleteByTag(Map<String, Object> inputs) throws StorageModuleException {
        String[] tags = getStringArrayFromObject(TAGS_KEY_NAME, inputs);
        long[] timestamp = getLongArrayFromObject(BEFORE_KEY_NAME, inputs);
        return new DeleteByTagMessage(tags, timestamp[0]).onCommandMessage(context);
    }

    public Object getTimeSpan(Map<String, Object> inputs) throws StorageModuleException {
        String queryChannel = getRequiredSetParameter(QUERY_CHANNEL_KEY_NAME, inputs).toString();
        return new GetTimeSpan(queryChannel).onCommandMessage(context);
    }

    public Object listTag(Map<String, Object> inputs) throws StorageModuleException {
        String query = getRequiredSetParameter(QUERY_KEY_NAME, inputs).toString();
        return new ListTagMessage(query).onCommandMessage(context);
    }

    public Object tag(Map<String, Object> inputs) throws StorageModuleException {
        String[] tags = getStringArrayFromObject(TAGS_KEY_NAME, inputs);
        long[] ids = getLongArrayFromObject(IDS_KEY_NAME, inputs);
        return new TagMessage(tags, ids).onCommandMessage(context);
    }

    public Object untag(Map<String, Object> inputs) throws StorageModuleException {
        String[] tags = getStringArrayFromObject(TAGS_KEY_NAME, inputs);
        long[] ids = getLongArrayFromObject(IDS_KEY_NAME, inputs);
        return new UntagMessage(tags, ids).onCommandMessage(context);
    }

    public Object deleteSession(Map<String, Object> inputs) throws StorageModuleException {
        Long sessionId = (Long) getRequiredSetParameter(SESSION_ID_KEY_NAME, inputs);
        return new DeleteSession(sessionId).onCommandMessage(context);
    }

    public Object get(Map<String, Object> inputs) throws StorageModuleException {
        Long sessionId = (Long) getRequiredSetParameter(SESSION_ID_KEY_NAME, inputs);
        String replayChannel = getRequiredSetParameter(REPLAY_CHANNEL_KEY_NAME, inputs).toString();
        Integer limit = (Integer) inputs.getOrDefault(LIMIT_KEY_NAME, null);
        Integer skip = (Integer) inputs.getOrDefault(SKIP_KEY_NAME, null);
        return new GetMessage(sessionId, limit, skip, replayChannel).onCommandMessage(context);
    }

    public Object queryBySessionId(Map<String, Object> inputs) throws StorageModuleException {
        Long sessionId = (Long) getRequiredSetParameter(SESSION_ID_KEY_NAME, inputs);
        Boolean onlyWithActiveMessages = (Boolean) inputs.getOrDefault(ONLY_WITH_ACTIVE_MESSAGE_KEY_NAME, null);
        return onlyWithActiveMessages == null ?
            new QueryMessage(sessionId).onCommandMessage(context) :
            new QueryMessage(sessionId, onlyWithActiveMessages).onCommandMessage(context);
    }

    public Object queryByChannelName(Map<String, Object> inputs) throws StorageModuleException {
        String queryChannel = getRequiredSetParameter(QUERY_CHANNEL_KEY_NAME, inputs).toString();
        return new QueryMessage(queryChannel).onCommandMessage(context);
    }

    public Object rename(Map<String, Object> inputs) throws StorageModuleException {
        Long sessionId = (Long) getRequiredSetParameter(SESSION_ID_KEY_NAME, inputs);
        return new RenameMessage(sessionId).onCommandMessage(context);
    }

    public Object size(Map<String, Object> inputs) throws StorageModuleException {
        Long sessionId = (Long) getRequiredSetParameter(SESSION_ID_KEY_NAME, inputs);
        return new SizeMessage(sessionId).onCommandMessage(context);
    }

    public Object start(Map<String, Object> inputs) throws StorageModuleException {
        String listenChannel = getRequiredSetParameter(LISTEN_CHANNEL_KEY_NAME, inputs).toString();
        Integer maxBufferPeriodInSec = (Integer) inputs.getOrDefault(MAXIMUM_BUFFERING_PERIOD_IN_SEC, null);
        return maxBufferPeriodInSec != null
                ? new StartMessage(listenChannel, maxBufferPeriodInSec).onCommandMessage(context)
                : new StartMessage(listenChannel).onCommandMessage(context);
    }

    public Object stop(Map<String, Object> inputs) throws StorageModuleException {
        Long sessionId = (Long) getRequiredSetParameter(SESSION_ID_KEY_NAME, inputs);
        return new StopMessage(sessionId).onCommandMessage(context);
    }

    Object getRequiredSetParameter(String keyName, Map inputs) throws StorageModuleException {
        if (inputs == null) {
            throw new StorageModuleException("Input map was null.");
        }

        Object value = inputs.get(keyName);
        verifyRequiredValueIsNotNull(keyName, value);
        return value;
    }

    /**
     * Retrieves the given key from the map and converts the resulting object into a String array.
     *
     * @param key key we are looking for in the map.
     * @param inputs map containing key/value pairs
     * @return Resulting String array.
     */
    private String[] getStringArrayFromObject(String key, Map<String, Object> inputs) throws StorageModuleException {
        ArrayList<String> values = (ArrayList<String>) getRequiredSetParameter(key, inputs);
        return values.toArray(new String[values.size()]);
    }

    /**
     * Retrieves the given key from the map and converts the resulting object into a Long array.
     *
     * @param key key we are looking for in the map.
     * @param inputs map containing key/value pairs
     * @return Resulting Long array.
     */
    private long[] getLongArrayFromObject(String key, Map<String, Object> inputs) throws StorageModuleException {
        return ((Collection<Integer>) getRequiredSetParameter(key, inputs)).stream().mapToLong(Long::valueOf).toArray();
    }

    private static void verifyRequiredValueIsNotNull(String keyName, Object value) throws StorageModuleException {
        if (value == null) {
            throw new StorageModuleException(String.format("Required parameter %s was not specified.", keyName));
        }
    }
}
