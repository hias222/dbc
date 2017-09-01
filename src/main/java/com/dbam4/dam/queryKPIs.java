package com.dbam4.dam;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class queryKPIs implements Runnable {


    private static final Log LOGGER = LogFactory.getLog(queryKPIs.class);

    private parameter param;
    private int instance_nr;

    private Connection InstanceConnection;
    private getClob getSourceData;

    private volatile String lastrun;
    private volatile Timestamp dbDate;

    private String thread_sessiondir;
    private String thread_script;
    private int thread_run;
    private int thread_wait;

    private int intervall;

    private ExecutorService kpiservice;

    public queryKPIs(parameter param, int Node) throws IOException, URISyntaxException {

        //we said Node so calculate -1 depends on array counting in java
        this.instance_nr = Node - 1;
        //get new connection
        LOGGER.info("queryKPIs Init Instance " + Node + " Array " + instance_nr);
        this.param = param;
        this.intervall = param.getIntervall();

        //here we must ad some actual infos of instance, maybe it restarted etc.

        InstanceConnection = param.getSourceConnection(instance_nr);
        //this.lastrun = param.getLastRun(instance_nr);
        this.lastrun = param.getStartupTime(instance_nr);
        this.dbDate = param.getLastRunDate(instance_nr);

        this.thread_sessiondir = param.getSessionDir(instance_nr);
        this.thread_script = param.getSessionScript();
        this.thread_run = param.SessionsRun();
        this.thread_wait = param.SessionsWait();

        kpiservice = Executors.newFixedThreadPool(2);
        LOGGER.debug("init end KPI instance_nr " + (instance_nr + 1));


    }

    @Override
    public void run() {
        // TODO Implement this method
        Future F_SourceData;

        LOGGER.info("Start running instance KPI collection on Node " + (instance_nr + 1));
        //LOGGER.info("Here we need more data - time ");


        if (InstanceConnection == null) {
            try {
                InstanceConnection.close();
            } catch (SQLException e) {
                LOGGER.error(e);
            }
            LOGGER.error("Connection Problem internal nr " + (instance_nr + 1));
            InstanceConnection = param.getSourceConnection(instance_nr);
        }


        getSourceData = new getClob(param, instance_nr, this.dbDate);
        getSourceData.setConnection(InstanceConnection);

        LOGGER.debug("KPI starting instance_nr " + (instance_nr + 1));
        F_SourceData = kpiservice.submit(getSourceData);


        int i = 0;


        // Get nearest hour
        //            Date nearestHour   = DateUtils.round(now, Calendar.HOUR);
        //            System.out.println("nearestHour = " + formatter.format(nearestHour));

        int numberworking = 0;

        while (true) {

            i++;


            if (InstanceConnection == null) {
                try {
                    InstanceConnection.close();
                } catch (SQLException e) {
                    LOGGER.error(e);
                }
                LOGGER.error("Connection Problem internal nr " + (instance_nr + 1));
                InstanceConnection = param.getSourceConnection(instance_nr);
            } else {
                LOGGER.debug("Instance Connection succesfull");
            }

            //this.dbDate = param.getLastRunDate(instance_nr);
            //LOGGER.debug("+++++++ starting instance_nr " + (instance_nr + 1));

            if (kpiservice.isShutdown()) {

                LOGGER.debug("old session ended instance_nr" + (instance_nr + 1));
                kpiservice = Executors.newFixedThreadPool(1);

                //InstanceConnection2,
                this.lastrun = "run " + getSourceData.getLastRun();
                LOGGER.debug("DB state " + this.lastrun + " instance_nr " + (instance_nr + 1));

                // if everything is normal we gather data from v$sql
                LOGGER.debug("run Getting Source Data instance_nr " + (instance_nr + 1));
                F_SourceData = kpiservice.submit(getSourceData);

                // we not wait till th end
                LOGGER.info("query KPI Session run " + i + " on node " + (instance_nr + 1) + " end");


            } else {
                LOGGER.debug("old session running instance we wait maybe first start after long time " + (instance_nr + 1));
                //old servcie is running
                // we wait one time and stop it next tiome
                numberworking++;
                if (numberworking > 5) {
                    numberworking = 0;
                    LOGGER.debug("shutdown at instance" + (instance_nr + 1));
                    kpiservice.shutdown();
                } else {
                    LOGGER.debug("waited  " + numberworking + " round at instance" + (instance_nr + 1));
                }

                //try {
                //    kpiservice.awaitTermination(10, TimeUnit.SECONDS);
                //} catch (InterruptedException e) {
                //    LOGGER.error("Fialure during exitb kpi thread " + e);
                //}


            }

            LOGGER.debug("KPI check time instance " + (instance_nr + 1));


            try {

                int waits = getWaitMilli();
                LOGGER.debug("KPI Wait till next execution " + waits + " instance " + (instance_nr + 1));


                if (waits > 60000) {
                    LOGGER.debug("Waiting 60s instance " + (instance_nr + 1));
                    Thread.sleep(60000);
                    kpiservice.shutdown();
                    waits = waits - 60000;
                }

                LOGGER.info("Waiting " + Math.round(waits / 1000) + " seconds instance " + (instance_nr + 1));
                Thread.sleep(waits);


            } catch (InterruptedException e) {
                LOGGER.error(e.toString());
            }


        }

    }


    private int getWaitMilli() {


        Date now = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, 61);
        int modulo = calendar.get(Calendar.MINUTE);
        //LOGGER.info("Modulo  " + modulo + " intervall " + intervall);

        int nextstart = 0;

        int diffbase = (int) (calendar.getTimeInMillis() - now.getTime());

        if (modulo > 0) {
            nextstart = (int) Math.round((Math.floor(modulo / intervall) + 1) * intervall);
            //LOGGER.info("Modulo  " + nextstart);
            calendar.add(Calendar.MINUTE, -modulo);
        }

        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.MINUTE, nextstart);

        int diff = (int) (calendar.getTimeInMillis() - now.getTime());

        LOGGER.info("Next start " + (calendar.getTime()) + " now " + now + " diff in ms " + diff );


        if (diff < 0) {
            diff = 1;
        }

        return diff;
    }
}
