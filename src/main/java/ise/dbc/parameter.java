package ise.dbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import java.io.IOException;

import java.io.InputStream;

import java.io.InputStreamReader;


import java.net.URISyntaxException;


import java.nio.file.Files;


import java.nio.file.Paths;

import java.sql.Connection;


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.sql.Statement;

import java.sql.Timestamp;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;


import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;


public class parameter {


    // see http://www.javaquery.com/2015/10/how-to-read-properties-file-from.html

    private static final Log LOGGER = LogFactory.getLog(ise.dbc.parameter.class);

    //  private String PropertiesFile;

    private String S_ORACLE_TNS = "localhost";
    private String[] S_ORACLE_JDBCS;
    private int S_NUMBER_INSTANCES = 1;

    private String S_ORACLE_USER = "system";
    private String S_ORACLE_PWD = "oracle";


    // Attention must be in java files and right deployment
    private String A_Session_Script = "/sql/dbc_sessions_java.sql";

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

            FileInputStream in = new FileInputStream(PropertiesFile);
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
            } else {
                //databaseoutput.connection.url

                getDetailedTableData(properties);

                position_read = "analyticserver.metadata.cleanafterhours";
                D_clean_hours = Integer.parseInt(properties.getProperty("analyticserver.metadata.cleanafterhours"));

                position_read =
                        "databaseoutput.commiteverylines (The values are inserted in batch mode. After ths numbers of rows the insert starts.";
                D_commit_lines = Integer.parseInt(properties.getProperty("databaseoutput.commiteverylines"));

                //databaseoutput.type
                position_read = "databaseoutput.type (JDBC, LOCAL, HDFS)";
                S_TYPE = properties.getProperty("databaseoutput.type");
                //JDBC, LOCAL, HDFS
                this.WriteToDB = false;
                this.WriteToHDFS = false;
                this.WriteToLocal = false;
                if (S_TYPE.equalsIgnoreCase("JDBC")) {

                    LOGGER.debug("USE JDBC");

                    this.WriteToDB = true;

                    //position_read = "databaseoutput.SQLIDChangeHours";
                    //D_change_hours = Integer.parseInt(properties.getProperty("databaseoutput.SQLIDChangeHours"));

                    //analyticserver.metadata.cleanafterhours


                    this.SetReadPosition("databaseoutput.connection.url");
                    S_ORACLE_TNS = properties.getProperty("databaseoutput.connection.url");
                    S_ORACLE_USER = properties.getProperty("databaseoutput.user");
                    S_ORACLE_PWD = properties.getProperty("databaseoutput.password");


                }

                if (S_TYPE.equalsIgnoreCase("LOCAL")) {
                    LOGGER.debug("USE LOCAL");
                    position_read = "databaseoutput.basedirectory";
                    S_BaseDir = properties.getProperty("databaseoutput.basedirectory");

                    //check dir access
                    if (!Files.isDirectory(Paths.get(S_BaseDir))) {
                        LOGGER.error(S_BaseDir + " is not a directory chek " + position_read + " Parameter");
                        throw new IOException();
                    }

                    this.WriteToLocal = true;
                }

                if (S_TYPE.equalsIgnoreCase("HDFS")) {
                    LOGGER.debug("USE HDFS");
                    this.WriteToHDFS = true;
                    //base_config_dir


                    //S_BaseDir
                    position_read = "databaseoutput.basedirectory";
                    S_BaseDir = properties.getProperty("databaseoutput.basedirectory");
                    //S_HDFSConfigDir
                    position_read = "databaseoutput.hdfsconfigdir";
                    S_HDFSConfigDir = properties.getProperty("databaseoutput.hdfsconfigdir");

                }


                position_read = "databaseoutput.sqlsearchstring";
                S_SQLSearchString = properties.getProperty("databaseoutput.sqlsearchstring");
                position_read = "databaseoutput.sessionsearchstring";
                S_SESSIONSearchString = properties.getProperty("databaseoutput.sessionsearchstring");

                position_read = "databaseoutput.sqltexttable";
                S_sqltexttable = properties.getProperty("databaseoutput.sqltexttable");
                position_read = "databaseoutput.sessiontable";
                S_sessiontable = properties.getProperty("databaseoutput.sessiontable");

                position_read = "databaseoutput.tableresresolution";
                S_ResolutionTab = properties.getProperty("databaseoutput.tableresresolution");
                position_read = "databaseoutput.tablecolresolution";
                S_ResolutionCol = properties.getProperty("databaseoutput.tablecolresolution");


                //input.dir
                //the dir were files searhed for annalyse
                position_read = "input.dir";
                S_inputdir = properties.getProperty("input.dir");


                position_read = "databaseoutput.sessiontable.fields";
                SESSION_TABLE = properties.getProperty("databaseoutput.sessiontable.fields").split(",");
                // lets try to find field configs

                position_read = "databaseoutput.sessiontable.aggregates possible values sql_exec_num,sql_prev_num";
                SESSION_TABLE_AGGS = properties.getProperty("databaseoutput.sessiontable.aggregates").split(",");


                SESSION_TABLE_DEFINITION = new String[SESSION_TABLE.length][3];

                String l_tablename = "start";
                try {
                    for (int i = 0; i < SESSION_TABLE.length; i++) {
                        l_tablename = SESSION_TABLE[i];
                        String propertyvalue = "databaseoutput.sessiontable.fields." + SESSION_TABLE[i];
                        SESSION_TABLE_DEFINITION[i] = properties.getProperty(propertyvalue).split(",");
                    }
                } catch (java.lang.NullPointerException e) {
                    LOGGER.error("Check your properties at databaseoutput.sessiontable.fields." + l_tablename);
                    System.exit(1);
                }

                //reading kpi informations

                try {

                    LOGGER.info("Loding KPI data ");

                    for (String base_name : VSQL_BASE) {
                        this.SetReadPosition("databaseoutput.kpi." + base_name + ".fields");
                        String[] BASE_TABLE =
                                properties.getProperty("databaseoutput.kpi." + base_name + ".fields").split(",");


                        this.SetReadPosition("databaseoutput.kpi." + base_name + ".aggregation");
                        String[] BASE_aggregation =
                                properties.getProperty("databaseoutput.kpi." + base_name + ".aggregation").split(",");

                        this.SetReadPosition("databaseoutput.kpi." + base_name + ".keys");
                        String[] BASE_keys =
                                properties.getProperty("databaseoutput.kpi." + base_name + ".keys").split(",");

                        this.SetReadPosition("databaseoutput.kpi." + base_name + ".table");
                        String TABLE_NAME = properties.getProperty("databaseoutput.kpi." + base_name + ".table");

                        this.SetReadPosition("databaseoutput.kpi." + base_name + ".type");
                        String TABLE_TYPE = properties.getProperty("databaseoutput.kpi." + base_name + ".type");

                        this.SetReadPosition("database.metadata.kpi." + base_name);
                        String FILE_NAME = properties.getProperty("database.metadata.kpi." + base_name);


                        TableDef newentry = new TableDef();

                        newentry.TableName = TABLE_NAME;
                        newentry.TableType = TABLE_TYPE;
                        newentry.FILE_NAME = FILE_NAME;

                        newentry.Aggregates = BASE_aggregation;
                        newentry.Keys = BASE_keys;

                        LOGGER.info("KPI table length " + BASE_TABLE.length);


                        for (int i = 0; i < BASE_TABLE.length; i++) {

                            String row = BASE_TABLE[i];
                            this.SetReadPosition("databaseoutput.kpi." + base_name + ".fields." + row);
                            String[] propertyvalue =
                                    properties.getProperty("databaseoutput.kpi." + base_name + ".fields." + row).split(",");

                            ColDef newColumn = new ColDef(row);
                            newColumn.addCollTyps(Integer.parseInt(propertyvalue[0]), propertyvalue[1],
                                    propertyvalue[2]);
                            newentry.addColumnDef(newColumn);

                        }

                        LOGGER.debug(newentry.toDetailString());
                        getKPITableDefinition.add(newentry);

                    }

                } catch (java.lang.NullPointerException e) {
                    LOGGER.error("Check your properties at databaseoutput.kpi....fields.");
                    System.exit(1);
                }


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


    public int getNumberKPITableDefinition() {

        int number = 0;
        for (TableDef Tableitem : getKPITableDefinition) {
            //only one at the moment
            number++;
        }
        return number;
    }

    public List<TableDef> getKPITableDefinition() {
        return getKPITableDefinition;
    }

    public boolean checkOutputTables(parameter checkPram, Connection destination) {
        boolean working = true;
        String stmt;
        PreparedStatement ps;
        ResultSet rs = null;
        String TableName;

        String ALLTables = "";

        //first SQLText


        TableName = checkPram.getNameSQLTextTable();
        ALLTables = TableName;

        LOGGER.debug("check " + TableName);

        stmt = "select SQLID, SCHEMA_NAME,SQL_FULLTEXT, CREATION_TIME from " + TableName + " where rownum = 1";

        try {
            ps = destination.prepareStatement(stmt);
            rs = ps.executeQuery(stmt);
            LOGGER.debug("success");
        } catch (Exception e) {
            LOGGER.error("Failed query " + TableName);
            LOGGER.error(e);
            working = false;
            LOGGER.info("Create or change Table");
            LOGGER.error("create table " + TableName + "\n" + "        (\n" + "        SQLID VARCHAR2(13 BYTE),\n" +
                    "        SCHEMA_NAME VARCHAR2(30 BYTE),\n" + "        SQL_FULLTEXT CLOB,\n" +
                    "        CREATION_TIME date\n" + "        );");

        }


        TableName = checkPram.getS_ResolutionTab();

        ALLTables = ALLTables + ", " + TableName;

        LOGGER.debug("check " + TableName);

        stmt =
                "select SQLID, OBJECT_SCHEMA,OBJECT_NAME,OBJECT_TYPE, PARENT_OBJ_ID,PARENT_OBJ_SCHEMA," +
                        "PARENT_OBJ_NAME,PARENT_OBJ_TYPE,DEPTH from " + TableName + " where rownum = 1";

        try {
            ps = destination.prepareStatement(stmt);
            rs = ps.executeQuery(stmt);
            LOGGER.debug("success");
        } catch (Exception e) {
            LOGGER.error("Failed query " + TableName);
            LOGGER.error(e);
            working = false;
            LOGGER.info("Create or change Table");

            LOGGER.error("CREATE TABLE " + TableName + "\n" + "(\n" + "  ID NUMBER(10, 0) NOT NULL \n" +
                    ", SQLID VARCHAR2(64) \n" + ", OBJECT_SCHEMA VARCHAR2(64) \n" +
                    ", OBJECT_NAME VARCHAR2(1024) \n" + ", OBJECT_TYPE VARCHAR2(32) \n" +
                    ", PARENT_OBJ_ID NUMBER(10,0)\n" + ", PARENT_OBJ_SCHEMA VARCHAR2(64) \n" +
                    ", PARENT_OBJ_NAME VARCHAR2(64) \n" + ", PARENT_OBJ_TYPE VARCHAR2(64) \n" +
                    ", DEPTH NUMBER(5, 0) \n);");


        }

        //CREATE SEQUENCE VIEW_RES_IDSDS_RESULT_SEQ INCREMENT BY 1 START WITH 1;
        //select VIEW_RES_IDSDS_RESULT_SEQ.NEXTVAL from dual;

        stmt = "select " + TableName + "_SEQ.NEXTVAL from dual";

        try {
            ps = destination.prepareStatement(stmt);
            rs = ps.executeQuery(stmt);
            LOGGER.debug("success");
        } catch (Exception e) {
            LOGGER.error("Failed SEQUENCE " + TableName + "_SEQ");
            LOGGER.error(e);
            working = false;
            LOGGER.info("Create or change Sequence");

            LOGGER.error("CREATE SEQUENCE " + TableName + "_SEQ INCREMENT BY 1 START WITH 1;");


        }


        TableName = checkPram.getS_ResolutionCol();
        ALLTables = ALLTables + ", " + TableName;

        LOGGER.debug("check " + TableName);

        stmt = "select OBJECT_ID, COLUMN_NAME,COLUMN_USE from " + TableName + " where rownum = 1";

        try {
            ps = destination.prepareStatement(stmt);
            rs = ps.executeQuery(stmt);
            LOGGER.debug("success");
        } catch (Exception e) {
            LOGGER.error("Failed query " + TableName);
            LOGGER.error(e);
            working = false;
            LOGGER.info("Create or change Table");
            LOGGER.error("CREATE TABLE " + TableName + "\n" + "(\n" + "  OBJECT_ID NUMBER(10, 0) NOT NULL \n" +
                    ", COLUMN_NAME VARCHAR2(64) NOT NULL \n" + ", COLUMN_USE VARCHAR2(20) NOT NULL \n" + ");\n");


        }


        TableName = checkPram.getNameSessionTable();
        ALLTables = ALLTables + ", " + TableName;

        LOGGER.debug("check " + TableName);

        boolean start = true;

        List<String> necassaryFields =
                new ArrayList(Arrays.asList("SQL_ID", "SQL_CHILD_NUMBER", "SQL_PREV_ID", "PREV_CHILD_NUMBER"));

        int foundFields = 0;

        String CreateIt = "create table " + TableName + "\n(\n";
        stmt = "select ";

        for (ColDef item : checkPram.getFieldsSessionTable()) {

            Iterator<String> i = necassaryFields.iterator();
            while (i.hasNext()) {
                String o = (String) i.next();
                //some condition
                if (o.equalsIgnoreCase(item.getColumnName())) {
                    i.remove();
                }

            }

            //  if (item.getColumnName().equalsIgnoreCase("SQL_PREV_ID") ||
            //      item.getColumnName().equalsIgnoreCase("PREV_CHILD_NUMBER") ||
            //      item.getColumnName().equalsIgnoreCase("sql_prev_num")) {
            //      LOGGER.info("we don't need " + item.getColumnName());
            //  } else {

            if (!start) {
                stmt = stmt + ", \n";
                CreateIt = CreateIt + ", \n";
            }


            stmt = stmt + item.getColumnName();
            CreateIt = CreateIt + item.getColumnName() + " varchar(64) ";


            if (start) {
                start = false;

            }
            //}


        }


        for (String AGG_COL : this.getSessionsTableDefinition().Aggregates) {
            stmt = stmt + ",";
            stmt = stmt + "F_" + AGG_COL;
            CreateIt = CreateIt + ", " + "\nF_" + AGG_COL + " number ";
            //LOGGER.error("F_" + AGG_COL);
        }

        stmt = stmt + ",COLLECTION_DATE";
        CreateIt = CreateIt + ", \nCOLLECTION_DATE date ";

        stmt = stmt + " from " + TableName + " where rownum = 1 ";
        CreateIt = CreateIt + "\n );";


        try {
            ps = destination.prepareStatement(stmt);
            rs = ps.executeQuery(stmt);

            if (necassaryFields.size() == 0) {
                LOGGER.debug("success");
            } else {
                LOGGER.error("Missing necassary columns ");
                for (String item : necassaryFields) {
                    LOGGER.error(item);
                }
                working = false;

            }

        } catch (Exception e) {
            LOGGER.error("Failed query " + TableName);
            //LOGGER.info(stmt);
            LOGGER.error(e);
            working = false;
            LOGGER.error("Create or change Table");
            LOGGER.error(CreateIt);


        }

        //KPIs

        for (TableDef TableInfos : checkPram.getKPITableDefinition()) {

            TableName = TableInfos.TableName;

            ALLTables = ALLTables + ", " + TableName;

            start = true;
            LOGGER.debug("check " + TableName);

            stmt = " select COLLECTION_DATE, ";
            CreateIt = "create table " + TableName + "\n(\n";

            for (ColDef item : TableInfos.getListColumnDef()) {
                if (!item.getColumnDescription().equalsIgnoreCase("false")) {
                    if (!start) {
                        stmt = stmt + ", \n";
                        CreateIt = CreateIt + ", \n";
                    }

                    stmt = stmt + item.getColumnName();
                    if (item.getColumnName().equalsIgnoreCase("LAST_ACTIVE_TIME")) {
                        CreateIt = CreateIt + item.getColumnName() + " varchar2(64) ";
                    } else {
                        CreateIt = CreateIt + item.getColumnName() + " varchar2(64) ";
                    }


                    if (start) {
                        start = false;

                    }
                }

            }

            for (String AGG_COL : TableInfos.Aggregates) {
                stmt = stmt + ", \n";
                stmt = stmt + "F_" + AGG_COL;

                CreateIt = CreateIt + ",\n" + "F_" + AGG_COL + " number ";
            }

            stmt = stmt + " from " + TableName + " where rownum = 1 ";
            CreateIt = CreateIt + ", \nCOLLECTION_DATE date ";
            CreateIt = CreateIt + "\n );";

            try {
                ps = destination.prepareStatement(stmt);
                rs = ps.executeQuery(stmt);
                LOGGER.debug("success");
            } catch (Exception e) {
                LOGGER.error("Failed query " + TableName);
                LOGGER.error(e);
                working = false;
                LOGGER.info("Create or change Table");
                LOGGER.debug("Check Statement was " + stmt);
                LOGGER.error(CreateIt);


            }


        }

        LOGGER.info("SUCCESS on " + ALLTables);


        return working;
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

    /*
    public String SessionDir() {
        return this.A_SESSION_FILEDIR;


    }
*/

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

    public int getKPInumbers() {
        return this.VSQL_NAME.size();
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


    public String[] searchSessionDirs() {
        //get Dirs out of Filesystem
        String[] SessionDirs = new String[this.S_NUMBER_INSTANCES];

        // starting at A_BASE_DIR we search for data


        return SessionDirs;
    }

    public String getNameSessionTable() {
        return S_sessiontable;
    }

    public String getNameSQLTextTable() {
        return S_sqltexttable;
    }

    public TableDef getSessionsTableDefinition() {
        TableDef newentry = new TableDef();

        Set<String> uniqueColumnAggs = new HashSet<String>();
        List<String> Keys = new ArrayList<String>();

        newentry.TableName = this.S_sessiontable;
        newentry.TableType = "SESSION";
        newentry.FILE_NAME = "v$session";

        newentry.Aggregates = SESSION_TABLE_AGGS;


        //database,instance,username,sql_id,sql_child_number,sql_prev_id,PREV_CHILD_NUMBER

        String[] standard_keys = new String[]{
                "database", "instance", "username", "sql_id", "sql_child_number", "sql_prev_id", "PREV_CHILD_NUMBER"
        };


        //thats wrong  minus AGGS ??

        for (String item : standard_keys) {
            uniqueColumnAggs.add(item.toUpperCase());
        }

        for (String item : SESSION_TABLE) {
            if (uniqueColumnAggs.contains(item.toUpperCase())) {
                Keys.add(item);
            }
        }

        String[] keys = new String[Keys.size()];
        for (int i = 0; i < Keys.size(); i++)
            keys[i] = Keys.get(i);

        newentry.Keys = keys;

        LOGGER.debug("KPI table length " + SESSION_TABLE.length);
        for (ColDef items : this.getFieldsSessionTable()) {
            newentry.addColumnDef(items);
        }


        LOGGER.debug(newentry.toDetailString());
        return newentry;
    }

    public List<ColDef> getFieldsSessionTable() {

        List<ColDef> ColDescription;
        ColDescription = new ArrayList<ColDef>();

        //here changes needed

        for (int i = 0; i < this.SESSION_TABLE.length; i++) {
            ColDef Definition = new ColDef(this.SESSION_TABLE[i]);
            //Defenition.ColumnName = this.SESSION_TABLE[i];
            Definition.addCollTyps(Integer.parseInt(this.SESSION_TABLE_DEFINITION[i][0]),
                    this.SESSION_TABLE_DEFINITION[i][1], this.SESSION_TABLE_DEFINITION[i][2]);
            // Defenition.Type = this.SESSION_TABLE_DEFINITION[i][1];
            //   Defenition.ColumnDescription = this.SESSION_TABLE_DEFINITION[i][2];
            //   Defenition.CSVColumnOrder = Integer.parseInt(this.SESSION_TABLE_DEFINITION[i][0]);

            ColDescription.add(Definition);


        }

        return ColDescription;

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

    public int getCleanHours() {
        return this.D_clean_hours;
    }

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

    public String getSQLFileSearch() {
        return this.S_SQLSearchString;
    }

    public String getInputDir() {
        return this.S_inputdir;
    }


    public int usemaxPGAMB() {
        return this.A_max_pga_mb;
    }

    public String getSESSIONFileSearch() {
        return this.S_SESSIONSearchString;
    }


    private String SessionScript() throws IOException, URISyntaxException {


        InputStream in = this.getClass().getResourceAsStream(this.A_Session_Script);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        String xml = "--- loaded fom file \n";
        while ((line = reader.readLine()) != null) {
            // do something with the line here
            xml = xml + line + "\n";
            //System.out.println("Line read: " + line);


        }

        //System.out.println(xml);

        //byte[] encoded2 = Files.readAllBytes(Paths.get(this.getClass().getResource("/ise/dbc/dbc_sessions_java.sql").toString()));
        //String test = new String(encoded2, Charset.defaultCharset());

        //mabey bug
        //java.net.URL url = this.getClass().getResource("/sql/dbc_sessions_java.sql");
        //java.nio.file.Path resPath;


        //System.out.println("URl " + url.toString());

        //resPath = Paths.get(url.toURI());

        //String xml = new String(Files.readAllBytes(resPath), Charset.defaultCharset());
        //System.out.println(xml);

        //byte[] encoded = Files.readAllBytes(Paths.get(this.A_Session_Script));
        //return new String(encoded, Charset.defaultCharset());

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


    public String BaseDir() {
        return this.A_BASE_DIR;
    }

    public String getSourceUserName() {
        return S_ORACLE_USER;
    }

    public Connection getSourceConnection() {

        OracleConnection OracleSource = new OracleConnection();

        Connection source; // = new Connection();

        LOGGER.debug("Source Connection to " + S_ORACLE_USER + "@" + S_ORACLE_TNS);

        try {
            source = OracleSource.openOracleConnection(S_ORACLE_USER, S_ORACLE_PWD, S_ORACLE_TNS);
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
