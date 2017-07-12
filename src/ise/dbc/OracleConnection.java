package ise.dbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.sql.SQLRecoverableException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.XMLConfigurationFactory;

public class OracleConnection {

    static {
        System.setProperty(XMLConfigurationFactory.CONFIGURATION_FILE_PROPERTY, "log4j2.xml");
    }

    private static final Logger LOGGER = LogManager.getLogger(ise.dbc.OracleConnection.class);

    private Connection conn = null;

    /**
     * Obtain a connection to the Oracle database.
     * @throws java.sql.SQLException
     */
    public Connection openOracleConnection(String dbUser, String dbPassword, String connectionURL) throws SQLException,
                                                                                                          IllegalAccessException,
                                                                                                          InstantiationException,
                                                                                                          ClassNotFoundException {

        
        String driver_class = "oracle.jdbc.driver.OracleDriver";
        
        if (connectionURL.startsWith("crate")) {
            driver_class = "io.crate.client.jdbc.CrateDriver"; 
            LOGGER.info("Using crate " + connectionURL);         
        } else {
            driver_class = "oracle.jdbc.driver.OracleDriver";
            LOGGER.info("using oracle " + connectionURL);
        }
        //String driver_class = "oracle.jdbc.driver.OracleDriver";
        //String driver_class = "io.crate.client.jdbc.CrateDriver";
        
        //connectionURL = null;

        Connection conn = null;

        try {
            Class.forName(driver_class).newInstance();
            //connectionURL =
            //      "jdbc:oracle:thin:@iselvn01.ise-informatik.de:1521:ELVNDB";
            conn = DriverManager.getConnection(connectionURL, dbUser, dbPassword);
            conn.setAutoCommit(true);
            LOGGER.debug("Connected " + connectionURL);


            return conn;
        } catch (IllegalAccessException e) {
            LOGGER.error("Illegal Access Exception: (Open Connection).");
            LOGGER.error(e);
            throw e;
        } catch (InstantiationException e) {
            LOGGER.error("Instantiation Exception: (Open Connection).");
            //e.printStackTrace();
            LOGGER.error(e);
            throw e;
        } catch (ClassNotFoundException e) {
            LOGGER.error("Class Not Found Exception: (Open Connection).");
            //e.printStackTrace();
            LOGGER.error(e);
            throw e;
        } catch (SQLRecoverableException e) {
            LOGGER.error("SQLRecoverableException: (Open Connection).");
            //e.printStackTrace();
            LOGGER.error(e);
            throw e;
        } catch (SQLException e) {
            LOGGER.error("Caught SQL Exception: (Open Connection).");
            //e.printStackTrace();
            LOGGER.error(e);
            throw e;
        } 

    }


}
