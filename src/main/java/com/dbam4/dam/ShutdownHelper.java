package com.dbam4.dam;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ShutdownHelper implements Runnable {


    private static final Log LOGGER = LogFactory.getLog(ShutdownHelper.class);

    public ShutdownHelper() {
        super();
    }

    @Override
    public void run() {
        DataTransfer.running = false;
        LOGGER.warn("shutdown started");
    }
}
