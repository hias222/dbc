package ise.dbc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;

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
