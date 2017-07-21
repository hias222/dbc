package ise.dbc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.*;

public class queryInstance implements Runnable {



    private static final Log LOGGER = LogFactory.getLog(ise.dbc.queryInstance.class);

    private Connection InstanceConnection;
    private Connection InstanceConnection2;

    private getSessionData getSessionThread;
    private getClob getSourceData;
    private ExecutorService service;
    private ExecutorService kpiservice;

    private parameter param;
    private int instance_nr;

    private volatile String lastrun;
    private volatile Timestamp dbDate;

    private String thread_sessiondir;
    private String thread_script;
    private int thread_run;
    private int thread_wait;
    private int PollingMinutesSession;

    public queryInstance(parameter param, int Node) throws RuntimeException, IOException, URISyntaxException {

        //we said Node so calculate -1 depends on array counting in java
        this.instance_nr = Node - 1;
        //get new connection
        LOGGER.info("queryInstance Init Instance " + Node + " Array " + instance_nr);
        this.param = param;

        //here we must ad some actual infos of instance, maybe it restarted etc.


        InstanceConnection = param.getSourceConnection(instance_nr);
        InstanceConnection2 = param.getSourceConnection(instance_nr);


        //this.lastrun = param.getLastRun(instance_nr);
        this.lastrun = param.getStartupTime(instance_nr);
        this.dbDate = param.getLastRunDate(instance_nr);

        this.thread_sessiondir = param.getSessionDir(instance_nr);
        this.thread_script = param.getSessionScript();
        this.thread_run = param.SessionsRun();
        this.thread_wait = param.SessionsWait();
        this.PollingMinutesSession = param.getA_Session_Polling();

        service = Executors.newFixedThreadPool(2);
        kpiservice = Executors.newFixedThreadPool(2);


    }

    @Override
    public void run() {
        //
        Future F_SessionData;
        //Future F_SourceData;

        boolean bkpi = false;

        LOGGER.info("Start running instance collection on Node " + (instance_nr + 1));
        LOGGER.info("Collect session every " + this.thread_wait + "ms");
        LOGGER.info("Save Session data every  " + this.thread_run + " collections");

        //before first start we must get the actual time out of DB
        //for (int i = 0; i < 15; i++) {

        int i = 0;

        if (InstanceConnection == null) {
            try {
                InstanceConnection.close();
            } catch (SQLException e) {
                LOGGER.error(e);
            }
            LOGGER.error("Connection Problem internal nr" + instance_nr);
            InstanceConnection = param.getSourceConnection(instance_nr);
        }

        getSessionThread =
                new getSessionData(InstanceConnection, this.thread_sessiondir, this.thread_script, this.thread_run,
                        this.thread_wait, this.PollingMinutesSession);


        while (true) {

            i++;

            LOGGER.info("Session run " + i + " on node " + (instance_nr + 1) + " start ...");

            if (InstanceConnection == null) {
                try {
                    InstanceConnection.close();
                } catch (SQLException e) {
                    LOGGER.error(e);
                }
                LOGGER.error("Connection Problem internal nr" + instance_nr);
                InstanceConnection = param.getSourceConnection(instance_nr);
            }


            //get last run date
            this.dbDate = param.getLastRunDate(instance_nr);

            /**
             * collection Session Data
             * start thread getSessionData
             */

            F_SessionData = service.submit(getSessionThread);
            LOGGER.debug("run Session submit " + instance_nr);

            //waiting for end
            try {
                //Wait till the end get()
                LOGGER.debug("run waiting " + instance_nr);
                LOGGER.debug("run Session output " + instance_nr + " " + F_SessionData.get());
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException " + e);
                service.shutdown();
                service = Executors.newFixedThreadPool(1);
            } catch (ExecutionException e) {
                LOGGER.error("Our Execution failed maybe retry db connection  ");
                LOGGER.error("ExecutionException " + e);
                try {
                    InstanceConnection.close();
                } catch (SQLException f) {
                    LOGGER.error("Close failed " + f);
                }
                service.shutdown();
            }

            //here we get our new time and query the frame between start and this
            /**
             * collection SQL KPIs Data
             * start thread getClob
             */
            LOGGER.debug("end old session");
            /*

            if (bkpi) {

                if (kpiservice.isShutdown()) {

                    LOGGER.debug("+++++++ old session ended");
                    kpiservice = Executors.newFixedThreadPool(1);

                    //InstanceConnection2,
                    this.lastrun = "run " + getSourceData.getLastRun();
                    LOGGER.debug("DB state " + this.lastrun);

                    // if everything is normal we gather data from v$sql
                    LOGGER.debug("run Getting Source Data");
                    F_SourceData = kpiservice.submit(getSourceData);
                    LOGGER.debug("run Getting Source Data ...");

                    // we not wait ill th end

                    LOGGER.info("KPI Session run " + i + " on node " + (instance_nr + 1) + " end");


                } else {
                    LOGGER.debug("+++++++ old session running");
                    kpiservice.shutdown();
                    //try {
                    //    kpiservice.awaitTermination(10, TimeUnit.SECONDS);
                    //} catch (InterruptedException e) {
                    //    LOGGER.error("Fialure during exitb kpi thread " + e);
                    //}
                }
            }
*/

        }


    }


    public void endqueryInstance() throws SQLException {

        try {
            // Wait a while for existing tasks to terminate
            if (!service.awaitTermination(60, TimeUnit.SECONDS)) {
                service.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!service.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            service.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }


        InstanceConnection.close();
    }
}
