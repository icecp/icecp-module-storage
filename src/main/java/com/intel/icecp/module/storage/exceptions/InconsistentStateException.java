package com.intel.icecp.module.storage.exceptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InconsistentStateException extends Exception {
    private static final Logger LOGGER = LogManager.getLogger();

    public InconsistentStateException(Throwable t) {
        super(t);
    }

    public InconsistentStateException(String errorMessage) {
        super(errorMessage);
        LOGGER.error(errorMessage);
    }
}
