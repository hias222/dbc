package ise.dbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.XMLConfigurationFactory;

public class ShutdownHelper implements Runnable {
    
    static {
        System.setProperty(XMLConfigurationFactory.CONFIGURATION_FILE_PROPERTY, "log4j2.xml");
    }

    private static final Logger LOGGER = LogManager.getLogger(ShutdownHelper.class);
    
    public ShutdownHelper() {
        super();
    }
    
    @Override
    public void run() {
        DataTransfer.running=false;
        LOGGER.warn("shutdown started");
    }
}
