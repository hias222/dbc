package com.dbam4.dam;

//
// com.dbam4.dam
//

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;


public class parameter {


    // see http://www.javaquery.com/2015/10/how-to-read-properties-file-from.html

    private static final Log LOGGER = LogFactory.getLog(parameter.class);

    //  private String PropertiesFile;

    private String S_ORACLE_TNS = "localhost";
    private String[] S_ORACLE_JDBCS;
    private int S_NUMBER_INSTANCES = 1;

    ClassLoader objClassLoader = null;

    private String S_ORACLE_USER = "system";
    private String S_ORACLE_PWD = "oracle";


    // Attention must be in java files and right deployment
    private String A_Session_Script = "dbc_sessions_java.sql";

    private String[] S_IGNORED_SCHEMAS = {"SYS", "SYSTEM"};

    private Integer A_Session_Wait = 100;
    private Integer A_Session_Runs = 100;
    private Integer A_Session_Polling = 30;

    public Integer getA_Session_Polling() {
        return A_Session_Polling;
    }

    private String A_BASE_DIR;
    private Integer A_max_number_load_sqls;

    private String DB_SESSION_SCRIPT;

    private String BASE_SESSIONDIR = "SESSION";
    private String BASE_SQLDIR = "SQL";

    //DBData
    private String[] DB_INSTANCE_NAME;
    private String[] DB_NAME;
    private long[] DB_ID;
    private int A_max_pga_mb;
    private int D_clean_hours;
    private String[] DB_STARTUP;

    private int D_commit_lines;

    public int getD_commit_lines() {
        return D_commit_lines;
    }

    private Timestamp[] dbDate;
    private boolean Source;

    public boolean isSource() {
        return Source;
    }

    private String S_TYPE;
    private boolean WriteToDB;

    public boolean isWriteToDB() {
        return WriteToDB;
    }

    public boolean isWriteToHDFS() {
        return WriteToHDFS;
    }

    public boolean isWriteToLocal() {
        return WriteToLocal;
    }

    private boolean WriteToHDFS;
    private boolean WriteToLocal;

    public String getS_TYPE() {
        return S_TYPE;
    }

    private String S_BaseDir;

    public String getS_BaseDir() {
        return S_BaseDir;
    }

    private String S_HDFSConfigDir;

    public String getS_HDFSConfigDir() {
        return S_HDFSConfigDir;
    }


    private String S_SQLSearchString;
    private String S_SESSIONSearchString;
    private String S_inputdir;

    private String S_sqltexttable;

    private String S_sessiontable;
    private String S_ResolutionTab;
    private String S_ResolutionCol;

    public String getS_ResolutionTab() {
        return S_ResolutionTab;
    }

    public String getS_ResolutionCol() {
        return S_ResolutionCol;
    }

    private int S_min_elapsed_time;

    private int intervall;

    public int getIntervall() {
        return intervall;
    }

    private String S_STATE_FILE;

    private String[] SESSION_TABLE;
    private String[] SESSION_TABLE_AGGS;
    private String[][] SESSION_TABLE_DEFINITION;

    private List<String> VSQL_NAME;
    private List<String[]> VSQL_TABLE;
    private List<String> VSQL_TIME;
    private List<String> VSQL_BASE;

    private List<TableDef> getKPITableDefinition;


    String position_read;


    /**
     * @param PropertiesFile
     * @param Source         true=Source false=destination repository
     */
    public parameter(String PropertiesFile, boolean Source) {
        // get properties out of db-connection.properties
        Properties properties = new Properties();

        objClassLoader = getClass().getClassLoader();


        this.Source = Source;

        //String position_read;

        position_read = null;

        VSQL_NAME = new ArrayList<String>();
        VSQL_TABLE = new ArrayList<String[]>();
        VSQL_TIME = new ArrayList<String>();
        VSQL_BASE = new ArrayList<String>();
        getKPITableDefinition = new ArrayList<TableDef>();

        try {

            // Load Properties


            FileInputStream in = new FileInputStream(objClassLoader.getResource(PropertiesFile).getFile());

            //FileInputStream in = new FileInputStream(PropertiesFile);
            properties.load(in);
            in.close();

            //Enumeration listprops = properties.propertyNames();


            A_BASE_DIR = properties.getProperty("analyticserver.metadata.baseDirectory");

            if (Source) {

                String key = "null";

                getDetailedTableData(properties);


                this.SetReadPosition("database.source.url");
                S_ORACLE_TNS = properties.getProperty("database.source.url");

                this.SetReadPosition("database.connection.urls");
                S_ORACLE_JDBCS = properties.getProperty("database.connection.urls").split(",");

                this.SetReadPosition("database.source.user");
                S_ORACLE_USER = properties.getProperty("database.source.user");

                this.SetReadPosition("database.source.password");
                S_ORACLE_PWD = properties.getProperty("database.source.password");

                this.SetReadPosition("database.schemas.ignored");
                S_IGNORED_SCHEMAS = properties.getProperty("database.schemas.ignored").split(",");

                this.SetReadPosition("analyticserver.metadata.statefile");
                S_STATE_FILE = properties.getProperty("analyticserver.metadata.statefile");
                if (this.S_STATE_FILE == null) {
                    LOGGER.error("missing directory for state file");
                    throw new IOException("STATE FILE");
                }

                this.SetReadPosition("analyticserver.metadata.minelapsed");
                S_min_elapsed_time = Integer.parseInt(properties.getProperty("analyticserver.metadata.minelapsed"));

                //A_SESSION_FILEDIR = A_BASE_DIR + "/" + properties.getProperty("analyticserver.metadata.sessionDirectory");
                this.SetReadPosition("analyticserver.metadata.sessionWait");
                A_Session_Wait = Integer.parseInt(properties.getProperty("analyticserver.metadata.sessionWait"));

                this.SetReadPosition("analyticserver.metadata.pollingSessionMinutes");
                A_Session_Polling =
                        Integer.parseInt(properties.getProperty("analyticserver.metadata.pollingSessionMinutes"));


                this.SetReadPosition("analyticserver.metadata.sessionWait");
                A_Session_Runs = Integer.parseInt(properties.getProperty("analyticserver.metadata.sessionRuns"));

                this.SetReadPosition("analyticserver.metadata.usemaxPGAMB");
                A_max_pga_mb = Integer.parseInt(properties.getProperty("analyticserver.metadata.usemaxPGAMB"));


                this.SetReadPosition("analyticserver.metadata.maxNumberLoadSQLs");
                A_max_number_load_sqls =
                        Integer.parseInt(properties.getProperty("analyticserver.metadata.maxNumberLoadSQLs"));
                //A_change_hours

                this.SetReadPosition("set number of instances");
                S_NUMBER_INSTANCES = S_ORACLE_JDBCS.length;

                //from here we create everything
                //SQL, Session, etc Dirs
                //BASE
                // ---> instance
                // --------> SQL,Session

                //first we must get some db informations like instance name, dbid, startupnumber , dbname

                this.SetReadPosition("instance ARRAY DIM ");
                this.DB_INSTANCE_NAME = new String[S_NUMBER_INSTANCES];

                this.DB_ID = new long[S_NUMBER_INSTANCES];


                this.DB_NAME = new String[S_NUMBER_INSTANCES];
                this.DB_NAME[0] = "unknown";

                this.DB_STARTUP = new String[S_NUMBER_INSTANCES];

                this.dbDate = new Timestamp[S_NUMBER_INSTANCES];

                this.SetReadPosition("get intervall for kpis analyticserver.metadata.kpiintervall");
                this.intervall = Integer.parseInt(properties.getProperty("analyticserver.metadata.kpiintervall"));


                //database.metadat.vsqlcolums
                //VSQL_TABLE

                //Check number of Source Tables

                this.SetReadPosition("Load Session script");
                this.DB_SESSION_SCRIPT = this.SessionScript();
            }

        } catch (IOException e) {
            LOGGER.error("Check your properties at " + position_read);
            LOGGER.error(e);
            System.exit(1);
        } catch (URISyntaxException e) {
            LOGGER.error("Check your properties at " + position_read);
            LOGGER.error(e);
            System.exit(1);
        } catch (java.lang.NullPointerException e) {
            LOGGER.error("Check your properties at " + position_read);
            LOGGER.error(e);
            System.exit(1);
        } catch (java.lang.NumberFormatException e) {
            LOGGER.error("Check your properties at " + position_read);
            LOGGER.error(e);
            System.exit(1);
        }


    }


    private void getDetailedTableData(Properties properties) {

        String key = "null";
        Enumeration listprops = properties.propertyNames();

        while (listprops.hasMoreElements()) {
            try {
                key = (String) listprops.nextElement();
                LOGGER.trace(key + " -- " + properties.getProperty(key));
                String Part1 = key.substring(0, key.indexOf("."));
                if (Part1.equalsIgnoreCase("database")) {
                    String key2 = key.substring(key.indexOf(".") + 1);
                    String Part2 = key2.substring(0, key2.indexOf("."));
                    if (Part2.equalsIgnoreCase("metadata")) {
                        String key3 = key2.substring(key2.indexOf(".") + 1);
                        String Part3 = key3.substring(0, key3.indexOf("."));
                        if (Part3.equalsIgnoreCase("kpi")) {
                            this.SetReadPosition("Loading kpi paramter ");
                            //LOGGER.debug(Part3);
                            String key4 = key3.substring(key3.indexOf(".") + 1);
                            if (key4.toUpperCase().indexOf("ACTIVE") > 0) {

                                String runsequence = key4.substring(key4.toUpperCase().indexOf("ACTIVE") + 6);
                                this.SetReadPosition(runsequence);

                                LOGGER.trace(" --- number " + runsequence + " in " + key4 + " " +
                                        key4.toUpperCase().indexOf("ACTIVE"));
                                String runname = key4.substring(0, key4.toUpperCase().indexOf("ACTIVE"));
                                LOGGER.trace(" --- name " + runname);

                                this.SetReadPosition("database.metadata.kpi." + runname + "Active" + runsequence);

                                String TEMP_ACTIVE =
                                        properties.getProperty("database.metadata.kpi." + runname + "Active" + runsequence);

                                if (TEMP_ACTIVE.equalsIgnoreCase("TRUE")) {
                                    this.SetReadPosition("database.metadata.kpi." + runname + "Col" + runsequence);

                                    String TEMP_TABLE[] =
                                            properties.getProperty("database.metadata.kpi." + runname + "Col" +
                                                    runsequence).split(",");

                                    this.SetReadPosition("database.metadata.kpi." + runname + runsequence);

                                    String TEMP_NAME =
                                            properties.getProperty("database.metadata.kpi." + runname + runsequence);

                                    this.SetReadPosition("database.metadata.kpi." + runname + "Time" + runsequence);

                                    String TEMP_TIME =
                                            properties.getProperty("database.metadata.kpi." + runname + "Time" +
                                                    runsequence);

                                    if (TEMP_TIME == null) {
                                        LOGGER.warn("no time where claus for table (" + position_read + ") " +
                                                TEMP_NAME);
                                    }

                                    VSQL_NAME.add(TEMP_NAME);
                                    VSQL_TABLE.add(TEMP_TABLE);
                                    VSQL_TIME.add(TEMP_TIME);
                                    VSQL_BASE.add(runname + runsequence);


                                    LOGGER.info("Columns for KPIs out of " + runname + runsequence);
                                } else {
                                    LOGGER.info("False on " + position_read);
                                }


                            }


                        }
                    }
                }

            } catch (java.lang.StringIndexOutOfBoundsException e) {
                LOGGER.trace("error on key " + key);
            }
        }

    }

    public String toString() {
        String output;

        if (Source) {

            output = "Source mode \n";

            if (this.DB_NAME[0] == "unknown") {
                output = "Found " + S_NUMBER_INSTANCES + " instances\n";
                output = output + S_ORACLE_JDBCS[0] + "\n";
                output = output + S_ORACLE_USER + "\n";
                output = output + S_ORACLE_PWD.substring(0, 2) + "XXXXX\n";
                output = output + "Base URL for analytic " + S_ORACLE_USER + "@" + S_ORACLE_TNS;

            } else {

                for (int i = 0; i < S_NUMBER_INSTANCES; i++) {
                    output = output + i + ": " + S_ORACLE_JDBCS[i] + "\n";
                    output =
                            output + "     " + this.DB_ID[i] + " " + this.DB_NAME[i] + " " + this.DB_INSTANCE_NAME[i] +
                                    " " + this.DB_STARTUP[i] + "\n";
                    output = output + "SQL Output Directory " + this.getSQLDir(i) + "\n";
                    output = output + "SESSION Output Directory " + this.getSessionDir(i) + "\n";
                }


                output = output + "ignoring Schemas";
                for (int i = 0; i < S_IGNORED_SCHEMAS.length; i++) {
                    output = output + S_IGNORED_SCHEMAS[i].toString() + " ";
                }

                output = output + "\n";
                output = output + "Base Directory " + A_BASE_DIR + "\n";
            }


        } else {
            output = "Destination mode \n";
            output = output + " SES Table\n";
            for (int i = 0; i < this.SESSION_TABLE.length; i++) {
                output =
                        output + " Column Name " + SESSION_TABLE[i] + ": " + SESSION_TABLE_DEFINITION[i][0] + " " +
                                SESSION_TABLE_DEFINITION[i][1] + " " + SESSION_TABLE_DEFINITION[i][2] + "\n";
            }
        }


        return output;
    }


    public void SetReadPosition(String ReadPosition) {

        LOGGER.debug("Reading " + ReadPosition);
        this.position_read = ReadPosition;

    }

    public String[] getInstancesNames() {
        //first get all Names

        for (int i = 0; i < S_NUMBER_INSTANCES; i++) {
            this.getDBData(i);
            this.createFileDirectories(i);
        }

        String[] InstanceNames = new String[S_NUMBER_INSTANCES];
        for (int i = 0; i < S_NUMBER_INSTANCES; i++) {
            InstanceNames[i] = this.DB_INSTANCE_NAME[i];

        }


        return InstanceNames;

    }


    public List<String[]> getKPIvsql() {
        LOGGER.debug("getting LIST TableColumns ");

        return this.VSQL_TABLE;

    }


    public List<String> getTableKPIvsql() {
        //first get all Names
        LOGGER.debug("getting LIST TableNames ");

        return this.VSQL_NAME;

    }

    public List<String> getTimeKPIvsql() {
        //first get all Names
        LOGGER.debug("getting LIST TimeColumn ");

        return this.VSQL_TIME;

    }


    public String getSessionDir(int internal_nr) {
        String BASE_OUTPUT =
                this.A_BASE_DIR + "/" + this.DB_NAME[internal_nr] + "/" + this.DB_INSTANCE_NAME[internal_nr];
        return BASE_OUTPUT + "/" + this.BASE_SESSIONDIR;
    }

    public String getSQLDir(int internal_nr) {
        String BASE_OUTPUT =
                this.A_BASE_DIR + "/" + this.DB_NAME[internal_nr] + "/" + this.DB_INSTANCE_NAME[internal_nr];
        return BASE_OUTPUT + "/" + this.BASE_SQLDIR;

    }


    public String getSessionScript() {
        return this.DB_SESSION_SCRIPT;
    }

    public int getMinElapsedTime() {
        return this.S_min_elapsed_time;
    }

    public int getMaxNumberLoadSQLs() {
        return this.A_max_number_load_sqls;
    }

    //A_change_hours


    public String getStartupTime(int internal_nr) {
        return this.DB_STARTUP[internal_nr];
    }

    public Timestamp getLastRunDate(int internal_nr) {
        return this.dbDate[internal_nr];
    }

    public void setLastRunDate(int internal_nr, Timestamp lastRun) {

        this.dbDate[internal_nr] = lastRun;

        LOGGER.debug("update time " + internal_nr);

        String instance_filename = this.S_STATE_FILE + "_" + internal_nr + ".txt";

        FileOutputStream out;
        try {

            FileInputStream in = new FileInputStream(instance_filename);
            Properties props = new Properties();
            props.load(in);
            in.close();


            out = new FileOutputStream(instance_filename);

            LOGGER.debug("open prop file" + instance_filename);
            String COL_TIME = "collection_time_" + DB_INSTANCE_NAME[internal_nr];
            LOGGER.debug("write prop " + COL_TIME);
            props.setProperty(COL_TIME, lastRun.toString());
            props.store(out, "State of instances");
            out.close();

        } catch (FileNotFoundException e) {

            LOGGER.error(e);
        } catch (java.io.IOException e) {

            LOGGER.error(e);
        } catch (Exception e) {
            LOGGER.error(e);

        }

        LOGGER.debug("updated load time for instance " + internal_nr);


    }


    public int usemaxPGAMB() {
        return this.A_max_pga_mb;
    }


    private String SessionScript() throws IOException, URISyntaxException {

        File tmp_file = null;

        //FileInputStream in = new FileInputStream(objClassLoader.getResource(PropertiesFile).getFile());

        InputStream in = objClassLoader.getResourceAsStream(this.A_Session_Script);
        //InputStream in = this.getClass().getResourceAsStream(this.A_Session_Script);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        String xml = "--- loaded fom file \n";
        while ((line = reader.readLine()) != null) {
            // do something with the line here
            xml = xml + line + "\n";
            //System.out.println("Line read: " + line);

        }

        //we write a example output
        //tmp_file = new File(this.A_BASE_DIR + "/output.txt");
        //tmp_file

        File fout = new File(this.A_BASE_DIR + "/output.txt");
        FileOutputStream fos = new FileOutputStream(fout);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        bw.write("Description of fields in output");
        bw.newLine();
        bw.write("sid ");
        bw.newLine();
        bw.write("NumberSnaps, NumberIO,  NumberCPU ");
        bw.newLine();

        bw.write("serial#, username, machine,terminal, osuser, program, state, sql_id, sql_child_number, ");
        bw.write("sql_exec_start, prev_sql_id, PREV_CHILD_NUMBER , prev_exec_start, module, action, ");
        bw.write("client_identifier, client_info, service_name, ecid, status, sql_exec_id, prev_exec_id");
        bw.newLine();
        bw.newLine();
        bw.write("NumberCPU");
        bw.newLine();
        bw.write("(tmp_sessions(i).state != 'WAITING') OR (tmp_sessions(i).state = 'WAITING' AND tmp_sessions(i).wait_class != 'Idle') ");

        bw.newLine();
        bw.write("NumberIO");
        bw.newLine();
        bw.write("tmp_sessions(i).wait_class = 'User I/O' ");
        bw.close();


        return xml;

    }

    public int SessionsWait() {
        return this.A_Session_Wait;
    }


    public int SessionsRun() {
        return this.A_Session_Runs;
    }

    public String[] IgnoredSchmes() {
        return this.S_IGNORED_SCHEMAS;
    }


    public String getSourceUserName() {
        return S_ORACLE_USER;
    }


    private void getDBData(int internal_nr) {

        //First we check if we have old runs
        //we search for some old instance run data

        try {

            Connection Base = this.getSourceConnection(internal_nr);

            String new_filename = this.S_STATE_FILE + "_" + internal_nr + ".txt";


            Statement stmt = Base.createStatement();
            String SqlText = "select instance_name, STARTUP_TIME from v$instance";
            //select INSTANCE_NAME, startup_time from v$instance;
            //select DBID, NAME from v$database;
            ResultSet rset = stmt.executeQuery(SqlText);

            while (rset.next()) {
                this.DB_INSTANCE_NAME[internal_nr] = (rset).getString("instance_name");
                this.DB_STARTUP[internal_nr] = (rset).getString("STARTUP_TIME");
                //System.out.println("Datum ....");
                //Timestamp timestamp = rset.getTimestamp("STARTUP_TIME");
                this.dbDate[internal_nr] = (rset).getTimestamp("STARTUP_TIME");
                //System.out.println("Datum " +  this.dbDate[internal_nr]);
            }

            SqlText = "select DBID, NAME from v$database";
            rset = stmt.executeQuery(SqlText);
            //4294967296 -8
            //2147483648 - 7
            //3619294444

            while (rset.next()) {
                this.DB_NAME[internal_nr] = (rset).getString("NAME");
                this.DB_ID[internal_nr] = (rset).getLong("DBID");
                //System.out.println((rset).getString("instance_name"));
            }

            Base.close();

            String COL_TIME = "collection_time_" + DB_INSTANCE_NAME[internal_nr];


            if (!Files.exists(Paths.get(new_filename))) {
                System.out.println("no old state avilable at " + new_filename);

                try {

                    Properties props = new Properties();

                    props.setProperty(COL_TIME, this.DB_STARTUP[internal_nr]);
                    props.store(new FileOutputStream(new File(new_filename)), " State of instances");

                } catch (IOException e) {
                    System.out.println(e);
                    System.exit(1);
                }
            } else {
                //read properties
                try {

                    FileInputStream in = new FileInputStream(new_filename);
                    Properties props = new Properties();
                    props.load(in);
                    in.close();
                    String LAST_COL_TIME;
                    LAST_COL_TIME = props.getProperty(COL_TIME);
                    if (LAST_COL_TIME == null) {
                        LOGGER.debug(" databse time write it back to properties file");
                        FileOutputStream out = new FileOutputStream(new_filename);
                        props.setProperty(COL_TIME, this.DB_STARTUP[internal_nr]);
                        props.store(out, "State of instances");
                        out.close();
                    } else {
                        //convert it to dbtime

                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                        LOGGER.debug(LAST_COL_TIME);
                        java.util.Date parsedDate = dateFormat.parse(LAST_COL_TIME);
                        this.dbDate[internal_nr] = new Timestamp(parsedDate.getTime());

                    }
                    LOGGER.info("LAST " + COL_TIME + " " + LAST_COL_TIME);
                } catch (IOException e) {
                    LOGGER.debug(DB_INSTANCE_NAME[internal_nr]);
                    LOGGER.error(e);
                    System.exit(1);
                } catch (ClassCastException e) {
                    LOGGER.debug(DB_INSTANCE_NAME[internal_nr]);
                    LOGGER.error(e);
                }

            }


        } catch (SQLException e) {

            System.out.println(e);
            LOGGER.error("Failure in connect please check node config " + internal_nr + " " + e);
            e.printStackTrace();
            //Base.close();
            //System.exit(1);
        } catch (RuntimeException e) {
            LOGGER.error("Failure in connect please check node config " + internal_nr + " " + e);
            e.printStackTrace();
            //System.exit(1);
        } catch (Exception e) {
            LOGGER.error("Failure in connect please check node config " + internal_nr + " " + e);
            e.printStackTrace();
            //System.exit(1);
        }

    }

    private void createFileDirectories(int instance_nr) {
        //check if directory exist BASE+dbname


        if (!Files.exists(Paths.get(this.A_BASE_DIR + "/" + this.DB_NAME[instance_nr]))) {
            System.out.println("we must create one ..." + this.A_BASE_DIR + "/" + this.DB_NAME[instance_nr]);

            try {
                Files.createDirectory(Paths.get(this.A_BASE_DIR + "/" + this.DB_NAME[instance_nr]));
            } catch (IOException e) {
                System.out.println(e);
                System.exit(1);
            }

        }

        if (!Files.exists(Paths.get(this.A_BASE_DIR + "/" + this.DB_NAME[instance_nr] + "/" +
                this.DB_INSTANCE_NAME[instance_nr]))) {
            System.out.println("we must create one ..." + this.A_BASE_DIR + "/" + this.DB_NAME[instance_nr] + "/" +
                    this.DB_INSTANCE_NAME[instance_nr]);

            try {
                Files.createDirectory(Paths.get(this.A_BASE_DIR + "/" + this.DB_NAME[instance_nr] + "/" +
                        this.DB_INSTANCE_NAME[instance_nr]));
            } catch (IOException e) {
                System.out.println(e);
                System.exit(1);
            }

        }

        String BASE_OUTPUT =
                this.A_BASE_DIR + "/" + this.DB_NAME[instance_nr] + "/" + this.DB_INSTANCE_NAME[instance_nr] + "/";

        if (!Files.exists(Paths.get(BASE_OUTPUT + this.BASE_SQLDIR))) {
            try {
                Files.createDirectory(Paths.get(BASE_OUTPUT + this.BASE_SQLDIR));
            } catch (IOException e) {
                System.out.println(e);
                System.exit(1);
            }
        }

        if (!Files.exists(Paths.get(BASE_OUTPUT + this.BASE_SESSIONDIR))) {
            try {
                Files.createDirectory(Paths.get(BASE_OUTPUT + this.BASE_SESSIONDIR));
            } catch (IOException e) {
                System.out.println(e);
                System.exit(1);
            }
        }


    }


    public Connection getSourceConnection(int instance_nr) {

        OracleConnection OracleSource = new OracleConnection();

        Connection source; // = new Connection();

        try {
            source = OracleSource.openOracleConnection(S_ORACLE_USER, S_ORACLE_PWD, S_ORACLE_JDBCS[instance_nr]);

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return source;
    }

}
