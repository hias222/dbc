package ise.dbc;

// -----------------------------------------------------------------------------
// CLOBFileExample.java
// -----------------------------------------------------------------------------

/*
 * =============================================================================
 * Copyright (c) 1998-2011 Jeffrey M. Hunter. All rights reserved.
 *
 * All source code and material located at the Internet address of
 * http://www.idevelopment.info is the copyright of Jeffrey M. Hunter and
 * is protected under copyright laws of the United States. This source code may
 * not be hosted on any other site without my express, prior, written
 * permission. Application to host any of the material elsewhere can be made by
 * contacting me at jhunter@idevelopment.info.
 *
 * I have made every effort and taken great care in making sure that the source
 * code and other content included on my web site is technically accurate, but I
 * disclaim any and all responsibility for any loss, damage or destruction of
 * data or any other property which may arise from relying on it. I will in no
 * case be liable for any monetary damages arising from such loss, damage or
 * destruction.
 *
 * As with any code, ensure to test this code in a development environment
 * before attempting to run it in production.
 * =============================================================================
 */

import java.sql.*;

import java.io.*;

import java.net.URISyntaxException;

import java.util.*;

// Needed since we will be using Oracle's CLOB, part of Oracle's JDBC extended
// classes. Keep in mind that we could have included Java's JDBC interfaces
// java.sql.Clob which Oracle does implement. The oracle.sql.CLOB class
// provided by Oracle does offer better performance and functionality.
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import java.util.concurrent.Executors;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import oracle.sql.*;

// Needed for Oracle JDBC Extended Classes
import oracle.jdbc.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.XMLConfigurationFactory;


public class DataTransfer {

    // too load it out of file system

    static {
        System.setProperty(XMLConfigurationFactory.CONFIGURATION_FILE_PROPERTY, "log4j2.xml");
    }

    private static final Logger LOGGER = LogManager.getLogger(ise.dbc.DataTransfer.class);

    public static boolean running;

    private String inputTextFileName = null;

    private Connection conn = null;

    private static String VERSIONDBC = "A1_00_30";


    /**
     * Default constructor used to create this object. Responsible for setting
     * this object's creation date, as well as incrementing the number instances
     * of this object.
     * @param args Array of string arguments passed in from the command-line.
     * @throws java.io.IOException
     */
    public DataTransfer(String PropertiesFile) throws IOException {

        inputTextFileName = PropertiesFile;


    }

    public void SetSourceConnection(Connection SConn) {

        conn = SConn;

    }


    /**
     * Close Oracle database connection.
     * @throws java.sql.SQLException
     */

    /*
    public void closeOracleConnection() throws SQLException {

        try {
            conn.close();
            System.out.println("Disconnected.\n");
        } catch (SQLException e) {
            System.out.println("Caught SQL Exception: (Closing Connection).");
            e.printStackTrace();
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e2) {
                    System.out.println("Caught SQL (Rollback Failed) Exception.");
                    e2.printStackTrace();
                }
            }
            throw e;
        }

    }
*/


    /**
     * Method used to print program usage to the console.
     */
    static public void usage() {
        System.out.println("\nUsage: java CLOBFileExample \"Text File Name\"\n");
    }


    /**
     * Validate command-line arguments to this program.
     * @param args Array of string arguments passed in from the command-line.
     * @return Boolean - value of true if correct arguments, false otherwise.
     */
    static public boolean checkArguments(String[] args) {

        if (args.length > 1) {
            return true;
        } else {
            return false;
        }

    }


    /**
     * Override the Object toString method. Used to print a version of this
     * object to the console.
     * @return String - String to be returned by this object.
     */
    public String toString() {

        String retValue;

        retValue = "retValue";
        //Input File         : " + inputTextFileName + "\n" +
        //--    "Output File (1)    : " + outputTextFileName1 + "\n" +
        //    "Output File (2)    : " + outputTextFileName2 + "\n" +
        //    "Database User      : " + dbUser;
        return retValue;

    }


    /**
     * Sole entry point to the class and application.
     * @param args Array of string arguments passed in from the command-line.
     */
    public static void main(String[] args) throws IOException {

        DataTransfer.running = true;


        DataTransfer cLOBFileExample = null;
        System.out.println("DBANALYTIC Start");
        System.out.println(LOGGER.getName());
        LOGGER.debug("Version " + VERSIONDBC + " start");
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Start");
            System.out.println("ENABLED");

        }

        String PropertiesFile;


        //OracleConnection OracleSource;


        PropertiesFile = "db-connection.properties";

        if (checkArguments(args)) {

            if (args.length > 1) {
                for (int i = 0; i < args.length; i++) {
                    //System.out.println("i " + args[i]);
                    if (args[i].equals("-d")) {
                        PropertiesFile = args[(i + 1)].toString();
                        System.out.println("Properties " + PropertiesFile);
                    }

                    if (args[i].equals("-help")) {
                        usage();
                    }

                }

            }

        }


        // get properties out of db-connection.properties
        Properties properties = new Properties();

        try {

            //cLOBFileExample = new DataTransfer(PropertiesFile);
            //OracleSource = new OracleConnection();

            parameter parameters = new parameter(PropertiesFile, true);

            LOGGER.info(parameters.toString());

            String[] instances = parameters.getInstancesNames();
            int NumberofInstances = parameters.getInstancesNames().length;

            int[] failedruns = new int[NumberofInstances];
            int[] failedKPIs = new int[NumberofInstances];

            if (parameters.isSource()) {
                LOGGER.info(parameters.toString());
            }


            LOGGER.info("main() " + NumberofInstances + " Instances");


            LOGGER.info("main() starting run version" + VERSIONDBC);
            //we create our threads dor every instance

            ExecutorService service;
            service = Executors.newFixedThreadPool(NumberofInstances);

            ExecutorService serviceKPI;
            serviceKPI = Executors.newFixedThreadPool(NumberofInstances);

            //queryInstance[] threads = new queryInstance[NumberofInstances];
            Future[] F_SessionData = new Future[NumberofInstances];
            Future[] F_KPIData = new Future[NumberofInstances];

            for (int inst = 0; inst < NumberofInstances; inst++) {
                LOGGER.info("main starting thread on " + instances[inst] + " run ");

                failedruns[inst] = 0;

                try {
                    F_SessionData[inst] = null;
                    queryInstance newinstance = new queryInstance(parameters, (inst + 1));
                    F_SessionData[inst] = service.submit(newinstance);
                } catch (RuntimeException e) {
                    failedruns[inst] = failedruns[inst] + 1;
                    LOGGER.error("Failure init queryInstance (" + failedruns[inst] + ")");

                }
                //F_SessionData[inst] = service.submit(new queryInstance(parameters, (inst + 1)));

                LOGGER.info("main finished starting thread on " + instances[inst] + " run ");
            }

            for (int inst = 0; inst < NumberofInstances; inst++) {
                LOGGER.info("KPI starting thread on " + instances[inst] + " run ");

                failedKPIs[inst] = 0;
                try {
                    F_KPIData[inst] = null;
                    queryKPIs newinstance = new queryKPIs(parameters, (inst + 1));
                    F_KPIData[inst] = serviceKPI.submit(newinstance);

                } catch (RuntimeException e) {
                    failedKPIs[inst] = failedKPIs[inst] + 1;
                    LOGGER.error("Failure init KPI queryInstance (" + failedKPIs[inst] + ")");

                }

                //F_KPIData[inst] = serviceKPI.submit(new queryKPIs(parameters, (inst + 1)));

                LOGGER.info("KPI finished starting thread on " + instances[inst] + " run ");
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHelper()));

            while (running) {
                //LOGGER.info("Loop ... " + NumberofInstances);


                for (int inst = 0; inst < NumberofInstances; inst++) {

                    if (F_SessionData[inst] == null) {
                        LOGGER.info("Instance not started " + inst);
                        try {
                            F_SessionData[inst] = null;
                            queryInstance newinstance = new queryInstance(parameters, (inst + 1));
                            F_SessionData[inst] = service.submit(newinstance);
                        } catch (RuntimeException e) {
                            failedruns[inst] = failedruns[inst] + 1;
                            LOGGER.error("Failure init queryInstance + (" + failedruns[inst] + ") max 4");

                        }
                    } else {

                        if (!F_SessionData[inst].isDone()) {
                            //Everything is running

                            failedruns[inst] = 0;
                            LOGGER.info("Session runnning on " + instances[inst] + " run @" + inst);
                        } else {
                            //sopmething is stopped, recreate it
                            failedruns[inst]++;

                            LOGGER.warn("restart of Session @" + inst + " failed " + failedruns[inst]);
                            //java.sql.SQLRecoverableException
                            try {
                                F_SessionData[inst] = null;
                                queryInstance newinstance = new queryInstance(parameters, (inst + 1));
                                F_SessionData[inst] = service.submit(newinstance);
                            } catch (RuntimeException e) {
                                failedruns[inst] = failedruns[inst] + 1;
                                LOGGER.error("Failure init queryInstance + (" + failedruns[inst] + ") max 4");

                            }

                            //F_SessionData[inst] = service.submit(new queryInstance(parameters, (inst + 1)));
                        }

                    }

                    if (failedruns[inst] > 4) {
                        LOGGER.error("error of Session @" + inst);
                        //service.shutdown();
                        running = false;
                    }

                    if (F_KPIData[inst] == null) {
                        LOGGER.info("KPI Instance not started " + inst);
                        try {
                            F_KPIData[inst] = null;
                            queryKPIs newinstance = new queryKPIs(parameters, (inst + 1));
                            F_KPIData[inst] = serviceKPI.submit(newinstance);

                        } catch (RuntimeException e) {
                            failedruns[inst] = failedruns[inst] + 1;
                            LOGGER.error("Failure init KPI queryInstance(" + failedKPIs[inst] + ") max 4");

                        }
                    } else {


                        if (!F_KPIData[inst].isDone()) {
                            //Everything is running

                            failedKPIs[inst] = 0;
                            LOGGER.info("KPI Session runnning on " + instances[inst] + " run @" + inst);
                        } else {
                            //sopmething is stopped, recreate it
                            failedKPIs[inst] = failedKPIs[inst] + 1;

                            LOGGER.warn("restart of KPI Session @" + inst + " failed " + failedKPIs[inst]);

                            try {
                                F_KPIData[inst] = null;
                                queryKPIs newinstance = new queryKPIs(parameters, (inst + 1));
                                F_KPIData[inst] = serviceKPI.submit(newinstance);

                            } catch (RuntimeException e) {
                                failedKPIs[inst] = failedKPIs[inst] + 1;
                                LOGGER.error("Failure init KPI queryInstance(" + failedKPIs[inst] + ") max 4");

                            }


                        }
                    }

                    if (failedKPIs[inst] > 4) {
                        LOGGER.error("KPI error of Session @" + inst);
                        running = false;
                    }

                }

                try {
                    TimeUnit.SECONDS.sleep(60);
                } catch (InterruptedException e) {
                    LOGGER.error("running instance " + e);
                }


            }

            try {

                LOGGER.info("stop of all services ");

                LOGGER.info("stop service ");
                service.shutdown();
                LOGGER.info("stop serviceKPI ");
                serviceKPI.shutdown();
                // Wait a while for existing tasks to terminate
                if (!service.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOGGER.info("stop service shutdownNow ");
                    service.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!service.awaitTermination(60, TimeUnit.SECONDS))
                        System.err.println("Pool did not terminate");
                }

                if (!serviceKPI.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOGGER.info("stop serviceKPI shutdownNow");
                    serviceKPI.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!serviceKPI.awaitTermination(60, TimeUnit.SECONDS))
                        System.err.println("Pool did not terminate");
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                service.shutdownNow();
                serviceKPI.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }

            //HOOKCheck??


        } catch (URISyntaxException e) {
            LOGGER.error("exception main");
            LOGGER.error(e);
            System.exit(1);
        }


        System.exit(1);
    }

}
