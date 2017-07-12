package ise.dbc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.io.OutputStreamWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.Timestamp;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import java.util.concurrent.TimeUnit;

import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleTypes;

import oracle.sql.CLOB;
import oracle.sql.DATE;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.XMLConfigurationFactory;


public class getClob implements Runnable {


    static {
        System.setProperty(XMLConfigurationFactory.CONFIGURATION_FILE_PROPERTY, "log4j2.xml");
    }

    private static final Logger LOGGER = LogManager.getLogger(ise.dbc.getClob.class);

    private Object then;


    private Connection conn;
    private String BaseTable;
    private String[] ignored_schemas;
    private String[] vsql_colums;
    private String table_name;
    private String time_col;
    private int instance_nr;

    private String Filename;

    private PreparedStatement psSelectSQL;

    private volatile Timestamp lastrun;
    private volatile Timestamp before;

    private volatile Timestamp lastrunKPI;
    private volatile Timestamp beforeKPI;

    private parameter param;


    private Integer connresets;
    private int minElapsed;

    private int chunksize;

    private List<String> VSQL_NAME;
    private List<String[]> VSQL_TABLE;
    private List<String> VSQL_TIME;


    /**
     * @param conn
     * @param ignored_schemas
     * @param id
     * @param schema
     * @param Filename
     */
    public getClob(parameter param, int instance_nr, Timestamp lastrun) {

        //this.conn = conn;
        this.ignored_schemas = param.IgnoredSchmes();
        this.Filename = param.getSQLDir(instance_nr);

        this.chunksize = param.getMaxNumberLoadSQLs();

        this.connresets = 0;

        this.instance_nr = instance_nr;
        this.before = lastrun;
        this.lastrun = lastrun;
        this.lastrunKPI = lastrun;

        VSQL_NAME = new ArrayList<String>();
        VSQL_TABLE = new ArrayList<String[]>();

        this.VSQL_TABLE = param.getKPIvsql();
        this.VSQL_NAME = param.getTableKPIvsql();
        this.VSQL_TIME = param.getTimeKPIvsql();

        this.param = param;

        this.minElapsed = param.getMinElapsedTime();

        if (this.minElapsed < 1) {
            this.minElapsed = 0;
        }

    }

    public void setConnection(Connection conn) {
        this.conn = conn;


    }


    public Timestamp getLastRun() {
        return this.lastrun;
    }

    /**
     * @param BaseName
     * Table BaseName vsql or vsql_plan
     */


    public void run() {
        LOGGER.info("Starting Loading SQL Data on node " + (instance_nr + 1));

        try {

            LOGGER.debug("last run time " + this.lastrun.toString() + " instance " + this.instance_nr);

            param.setLastRunDate(this.instance_nr, this.lastrun);
            //setting date for end
            LOGGER.debug("Get new run time");

            this.setCollectionTime();
            //we should do this during running
            //persist old time
            //need function in parameter file

            //get SQL Data


            String[][] COLUMNS = new String[this.VSQL_TABLE.size()][];
            this.VSQL_TABLE.toArray(COLUMNS);

            String[] NAMES = new String[this.VSQL_NAME.size()];
            this.VSQL_NAME.toArray(NAMES);

            String[] TIMES = new String[this.VSQL_TIME.size()];
            this.VSQL_TIME.toArray(TIMES);

            for (int i = 0; i < this.VSQL_TABLE.size(); i++) {
                this.table_name = NAMES[i];
                this.vsql_colums = COLUMNS[i];
                this.time_col = TIMES[i];
                LOGGER.info("Starting Loading KPIs " + this.table_name + "@instance" + (this.instance_nr + 1));
                this.getKPIFromSQL();

            }

            //get SQLL
            this.getCLOBFromSQL();
            LOGGER.debug("Save last run time " + this.instance_nr);
            param.setLastRunDate(this.instance_nr, this.lastrun);
            LOGGER.info("End Loading SQL Data on node " + (instance_nr + 1));


            if (this.getPGAUsage()) {

                //this.connresets = 0;
                LOGGER.debug("restart connection");
                //check connection
                if (this.conn.isReadOnly()) {
                    LOGGER.debug("READONLY");
                } else {
                    this.conn.close();
                    this.conn = param.getSourceConnection(this.instance_nr);
                    LOGGER.debug("new connection");
                }

            }

            this.connresets = 0;


        } catch (SQLException e) {
            LOGGER.error("Connection failed " + e);
            //restart it?

            try {
                LOGGER.info("try reconnect");
                if (this.conn.isReadOnly()) {
                    LOGGER.debug("READONLY");
                } else {
                    this.conn.close();
                    this.conn = param.getSourceConnection(this.instance_nr);
                    LOGGER.info("new connection");
                }
            } catch (SQLException f) {
                connresets++;
                LOGGER.error("Failed connection " + f);
                if (connresets > 4) {
                    LOGGER.error("Shutdown");
                    System.exit(0);
                }
            }
            //System.exit(0);
        } catch (Exception e) {
            LOGGER.error("run " + e);
            System.exit(0);
        }


    }


    private boolean getPGAUsage() {

        boolean output = false;

        Statement stmt = null;
        //stmt = conn.createStatement();
        String sqlText = null;
        //CallableStatement stmt = null;


        try {

            stmt = conn.createStatement();

            String username = param.getSourceUserName();


            sqlText =
                "select sum (round(p.pga_used_mem / 1024 / 1024)) PGA \n" + "  from V$process p, V$session s \n" +
                " where s.paddr = p.addr \n" + "   and s.username = upper('" + username + "') ";


            LOGGER.trace("getPGA " + sqlText);

            ResultSet rset = stmt.executeQuery(sqlText);

            LOGGER.trace("getPGAUsage getting result for instance" + this.instance_nr);

            while (rset.next()) {
                //xmlDocument = ((OracleResultSet) rset).getCLOB("FULLTEXT");

                LOGGER.trace("getPGAUsage row PGA");
                Integer value = 0;
                try {
                    value = (rset).getInt("PGA");
                    LOGGER.trace("getPGAUsage PGA " + value);
                } catch (Exception e) {
                    LOGGER.error("RSET next " + e);
                }

                output = false;

                if (value > param.usemaxPGAMB()) {
                    LOGGER.debug("reset conection necassary + PGA is " + value);
                    output = true;
                }


            }


            rset.close();
            stmt.close();
            //conn.commit();

            LOGGER.debug("Close PGA query");


        } catch (SQLException e) {
            //System.out.println("Caught SQL Exception: (Write CLOB value to file - Streams Method).");
            //System.out.println("SQL:\n" + sqlText);
            LOGGER.error("SQLEXECEPTION " + e);
            LOGGER.error(sqlText);
            //e.printStackTrace();
            //throw e;
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOGGER.error("other getPGAUsage " + e);
            //e.printStackTrace();


        }

        return output;


    }

    private void getKPIFromSQL() {

        //we only need time from
        // entries newer than last run
        //last is wrong


        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("run getKPIFromSQL()");
        }

        String sqlText = null;
        //Statement stmt = null;
        //CallableStatement stmt = null;
        ResultSet rset = null;
        CLOB xmlDocument = null;

        long clobLength;
        long position;
        int chunkSize;
        char[] textBuffer;

        String sqlid;

        int charsRead = 0;
        int charsWritten = 0;

        FileOutputStream outputFileOutputStream = null;
        OutputStreamWriter outputOutputStreamWriter = null;
        BufferedWriter outputBufferedWriter = null;

        int totCharsRead = 0;
        int totCharsWritten = 0;
        File outputTextFile1 = null;


        try {

            int numrows = 0;

            String Start_before = this.beforeKPI.toString().substring(0, beforeKPI.toString().indexOf("."));
            String Start_lastrun = this.lastrunKPI.toString().substring(0, lastrunKPI.toString().indexOf("."));
            
            LOGGER.info("get KPIs from " + Start_before + " " + Start_lastrun);


            SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMddHHmmss"); //dd/MM/yyyy
            //java.util.Date now = new java.util.Date();
            String strDate = sdfDate.format(this.lastrunKPI);

            if (this.prepareSelect()) {

                ((OraclePreparedStatement) psSelectSQL).setString(1, Start_before);
                rset = this.psSelectSQL.executeQuery();

            }


            //System.out.println(sqlText + "\n");
            //rset.next();

            LOGGER.debug("getKPIFromSQL getting result " + this.instance_nr);
            //this.Sessname = param.getSessionDir(internal_nr)

            //File tmp_file = new File(param.getSessionDir(this.instance_nr) + "/.tmp_" + this.table_name + "_time");

            LOGGER.debug("getKPIFromSQL rows " + numrows + " " + this.table_name + "@" + this.instance_nr);

            outputTextFile1 = new File(param.getSessionDir(this.instance_nr) + "/actual_" + this.table_name + ".txt");


            LOGGER.trace("getKPIFromSQL write to " + outputTextFile1);

            outputFileOutputStream = new FileOutputStream(outputTextFile1, true);

            outputOutputStreamWriter = new OutputStreamWriter(outputFileOutputStream);
            outputBufferedWriter = new BufferedWriter(outputOutputStreamWriter);

            while (rset.next()) {
                //xmlDocument = ((OracleResultSet) rset).getCLOB("FULLTEXT");

                numrows++;

                String output = "";


                LOGGER.trace("getKPIFromSQL row SQLKPI");
                String value = "";
                try {
                    value = (rset).getString("SQLKPI");
                    LOGGER.trace("getKPIFromSQL row " + value);
                } catch (Exception e) {
                    LOGGER.error("SQLKPI " + e);
                }

                output = value;


                outputBufferedWriter.write(output);
                outputBufferedWriter.write("\n");


            }


            outputBufferedWriter.close();
            outputOutputStreamWriter.close();
            outputFileOutputStream.close();

            rset.close();
            //stmt.close();
            //conn.commit();


            LOGGER.debug("new Name is " + param.getSessionDir(this.instance_nr) + "/KPI_" + this.table_name + "_" +
                         strDate + ".txt");

            //strDate
            File outputTextFile2 =
                new File(param.getSessionDir(this.instance_nr) + "/KPI_" + this.table_name + "_" + strDate +
                         ".txt");

            if (outputTextFile1.exists()) {
                Files.move(outputTextFile1.toPath(), outputTextFile2.toPath(), StandardCopyOption.ATOMIC_MOVE);
            }

            LOGGER.trace("writed KPIs to " + outputTextFile2);


        } catch (SQLException e) {
            //System.out.println("Caught SQL Exception: (Write CLOB value to file - Streams Method).");
            //System.out.println("SQL:\n" + sqlText);
            LOGGER.error("SQL " + e);
            //e.printStackTrace();
            //throw e;
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOGGER.error("other  getKPI " );
            e.printStackTrace();


        }
    }


    private boolean prepareSelect() {
        boolean result = false;

        String sqlText = "";

        try {

            LOGGER.info("getKPIFromSQL Node " + (this.instance_nr + 1) + " time " + this.beforeKPI.toString() +
                        " to now " + this.lastrunKPI.toString());

            sqlText = "select ";
            boolean first = true;

            for (int i = 0; i < this.vsql_colums.length; i++) {

                if (first) {
                    sqlText = sqlText + "'\"' ||";
                    sqlText = sqlText + this.vsql_colums[i];
                    first = false;
                } else {
                    sqlText = sqlText + " || '\";\"' || ";
                    if (this.vsql_colums[i].equalsIgnoreCase(this.time_col)) {
                        sqlText = sqlText + "to_char(" + this.vsql_colums[i] + ",'YYYY-MM-DD/HH24:MI:SS')";
                    } else {
                        sqlText = sqlText + this.vsql_colums[i];
                    }
                }
            }

            sqlText = sqlText + " || '\";'" + " SQLKPI from " + this.table_name;

            //Start_before
            sqlText = sqlText + " where " + this.time_col + " >= to_date( ? ,'YYYY-MM-DD/HH24:MI:SS')";


            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("prepare statment " + sqlText);
            }

            psSelectSQL = conn.prepareStatement(sqlText);

            result = true;
        } catch (Exception e) {
            LOGGER.error("prepare insert ");
            LOGGER.error(sqlText);
            LOGGER.error(e);
            result = false;
        }
        return result;
    }


    private String generateSQLforMonitoring() {

        String sqlText;

        sqlText =
            "/* Load data from v$sql ****************************" +
            "* from clobs into files ***************************" +
            "* dbanalytic db monitoring for sqls ***************************" + "*/" + "DECLARE \n" +
            "  c_sessions sys_refCursor;\n" + "start_time               VARCHAR2(30);\n" +
            "end_time               VARCHAR2(30);\n" + " begin\n";

        sqlText = sqlText + " start_time := ? ;" + " end_time := ?;";

        sqlText =
            sqlText + " -- DBANALYTIC GET CLOBS FROM SQL TO FILE \n" + "OPEN c_sessions FOR \n" +
            "-- select for clob in time window \n" +
            "select /* get SQL clob dbanalytic */ SQLID, date_first, FULLTEXT, PARSING_SCHEMA_NAME from (" +
            "select a.SQL_ID SQLID , b.FIRST_LOAD_TIME, b.SQL_FULLTEXT FULLTEXT,date_first, a.PARSING_SCHEMA_NAME" +
            "  from (select distinct t.SQL_ID, first_load_time," +
            " to_date(t.first_load_time,'YYYY-MM-DD/HH24:MI:SS') date_first , parsing_schema_name" + "  from v$sql t ";

        if (ignored_schemas.length > 0) {

            sqlText = sqlText + " where elapsed_time >  " + this.minElapsed + " and PARSING_SCHEMA_NAME not in ( ";

            for (int i = 0; i < ignored_schemas.length; i++) {
                if (i > 0) {
                    sqlText = sqlText + ",";
                }
                sqlText = sqlText + "'" + ignored_schemas[i].toUpperCase() + "'";
            }

        }

        sqlText =
            sqlText + "  ) " + " order by to_date(t.first_load_time,'YYYY-MM-DD/HH24:MI:SS') ) a," + " v$sql b" + " where b.SQL_ID = a.SQL_ID" +
            " and a.first_load_time = b.first_load_TIME and a.parsing_schema_name = b.parsing_schema_name) \n" +
            "where ";
        /*
        sqlText =
            sqlText + " date_first >= to_date( '" + Start_before + "','YYYY-MM-DD/HH24:MI:SS')" +
            " and date_first < to_date('" + Start_lastrun + "','YYYY-MM-DD/HH24:MI:SS')";
*/

        sqlText =
            sqlText + " date_first >= to_date( start_time ,'YYYY-MM-DD/HH24:MI:SS')" +
            " and date_first < to_date( end_time ,'YYYY-MM-DD/HH24:MI:SS')";

        sqlText = sqlText + " and rownum < " + this.chunksize + " ;\n";

        sqlText = sqlText + "? := c_sessions;\n" + " end;";

        return sqlText;
    }


    private void getCLOBFromSQL() {

        String sqlText = null;
        //Statement stmt = null;
        CallableStatement stmt = null;
        ResultSet rset = null;
        CLOB xmlDocument = null;

        long clobLength;
        long position;
        int chunkSize;
        char[] textBuffer;

        Timestamp SQLrundate;

        String sqlid;
        String prev_sqlid = "";
        int numdouble = 0;
        int charsRead = 0;
        int charsWritten = 0;

        FileOutputStream outputFileOutputStream = null;
        OutputStreamWriter outputOutputStreamWriter = null;
        BufferedWriter outputBufferedWriter = null;

        int totCharsRead = 0;
        int totCharsWritten = 0;
        File outputTextFile1 = null;

        SQLrundate = this.lastrun;

        try {

            //stmt = conn.createStatement();

         
            String Start_before = this.before.toString().substring(0, before.toString().indexOf("."));
            String Start_lastrun = this.lastrun.toString().substring(0, this.lastrun.toString().indexOf("."));
            
            LOGGER.info("getCLOBFromSQL Node " + (this.instance_nr + 1) + "  time " + Start_before + " " +
                        Start_lastrun);
            
            
            sqlText = this.generateSQLforMonitoring();

            stmt = conn.prepareCall(sqlText);

            LOGGER.trace(sqlText);

            stmt.setString(1, Start_before);
            stmt.setString(2, Start_lastrun);
            LOGGER.trace("set params time " + Start_before + " " + Start_lastrun);

            stmt.registerOutParameter(3, OracleTypes.CURSOR);
            LOGGER.trace("set output cursor");

            stmt.execute();
            LOGGER.trace("executed query");

            rset = (ResultSet) stmt.getObject(3);
            LOGGER.trace("get resultset");


            //stmt.finalize();

            //rset = stmt.executeQuery(this.generateSQLforMonitoring());

            //rset.next();
            int numrows = 0;
            numdouble = 0;
            while (rset.next()) {
                numrows++;
                xmlDocument = ((OracleResultSet) rset).getCLOB("FULLTEXT");
                sqlid = (rset).getString("SQLID");
                //String get_date = (rset).getString("date_first");

                if (sqlid.equalsIgnoreCase(prev_sqlid)) {
                    LOGGER.trace("DOUBLE SQL + " + sqlid);
                    numdouble++;
                } else {
                    prev_sqlid = sqlid;

                    SQLrundate = (rset).getTimestamp("date_first");
                    this.lastrun = SQLrundate;

                    clobLength = xmlDocument.length();
                    chunkSize = xmlDocument.getChunkSize();
                    textBuffer = new char[chunkSize];
                    LOGGER.trace(Filename + " " + clobLength + " " + chunkSize + " " + sqlid + " " + this.before +
                                 " to " + this.lastrun);

                    try {

                        outputTextFile1 = new File(Filename + "/SQL_" + sqlid + ".txt");


                        outputFileOutputStream = new FileOutputStream(outputTextFile1);

                        outputOutputStreamWriter = new OutputStreamWriter(outputFileOutputStream);
                        outputBufferedWriter = new BufferedWriter(outputOutputStreamWriter);


                        outputBufferedWriter.write(rset.getString("PARSING_SCHEMA_NAME"));

                        outputBufferedWriter.write("\n");


                        for (position = 1; position <= clobLength; position += chunkSize) {

                            // Loop through while reading a chunk of data from the CLOB
                            // column using the getChars() method. This data will be stored
                            // in a temporary buffer that will be written to disk.
                            charsRead = xmlDocument.getChars(position, chunkSize, textBuffer);

                            // Now write the buffer to disk.
                            outputBufferedWriter.write(textBuffer, 0, charsRead);

                            totCharsRead += charsRead;
                            totCharsWritten += charsRead;

                        }


                        //  chunkSize = xmlDocument.getChunkSize();
                        //textBuffer = new byte[chunkSize];
                    } catch (java.io.FileNotFoundException nf) {
                        LOGGER.error("Catch during internal rows, going on");
                        LOGGER.error(nf);

                    }
                    outputBufferedWriter.close();
                    outputOutputStreamWriter.close();
                    outputFileOutputStream.close();
                }
            }

            stmt.close();

            rset.close();

            //conn.commit();


            LOGGER.debug("+++++ " + numrows + "(" + numdouble + ") Setting new last run " + this.lastrun);
            LOGGER.debug("getCLOBFromSQL " + numrows + " rows at instance " + this.instance_nr);

            LOGGER.debug("Save last run time " + this.instance_nr);
            param.setLastRunDate(this.instance_nr, this.lastrun);
            LOGGER.info("End Loading SQL Data on node " + (instance_nr + 1));

            if ((numrows - numdouble) < numrows / 10) {

                this.chunksize = numrows + numdouble;
                LOGGER.debug("change chunk size to " + this.chunksize);

            }

            //return xmlDocument;


        } catch (SQLException e) {
            //System.out.println("Caught SQL Exception: (Write CLOB value to file - Streams Method).");
            //System.out.println("SQL:\n" + sqlText);
            //e.printStackTrace();
            LOGGER.error("SQL " + e);
            LOGGER.error(sqlText);
            //throw e;
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            //e.printStackTrace();
            LOGGER.error("FILE " + e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOGGER.error("IO " + e);
            //e.printStackTrace();
            throw new RuntimeException(e);

        } catch (Exception e) {
            LOGGER.error("other getCLOBFromSQL " );
            //e.printStackTrace();
            throw new RuntimeException(e);

        }

    }

    private void setCollectionTime() throws SQLException {
        Statement stmt = null;
        stmt = conn.createStatement();
        String sqlText = "select sysdate actual_date from dual";


        ResultSet rset = stmt.executeQuery(sqlText);
        

        this.before = this.lastrun;
        this.beforeKPI = this.lastrunKPI;
       
       LOGGER.info("Instance " + this.instance_nr + " Last Run Time " + this.lastrunKPI.toString() + " Maximum SQL Value " +this.lastrun.toString() );
       

        while (rset.next()) {
            this.lastrun = (rset).getTimestamp("actual_date");
            this.lastrunKPI = this.lastrun;
        }

        //conn.commit();
        rset.close();
        stmt.close();

    }


    public void getCLOBFromView(Connection conn, Number id, String schema, String Filename) throws IOException,
                                                                                                   SQLException {

        String sqlText = null;
        Statement stmt = null;
        ResultSet rset = null;
        Long xmlDocument = null;

        long clobLength;
        long position;
        int chunkSize;
        InputStream data;
        char[] textBuffer;

        String sqlid;
        String owner;

        int charsRead = 0;
        int charsWritten = 0;

        FileOutputStream outputFileOutputStream = null;
        OutputStreamWriter outputOutputStreamWriter = null;
        BufferedWriter outputBufferedWriter = null;

        int totCharsRead = 0;
        int totCharsWritten = 0;
        File outputTextFile1 = null;


        try {

            stmt = conn.createStatement();

            /*
            sqlText =
                "SELECT xml_document " +
                "FROM   test_clob " +
                "WHERE  id = " + id  + " " +
                "FOR UPDATE";
    */
            sqlText =
                "select t.view_name VIEWS, t.text_length, t.text TEXT , owner" + " from dba_views t " +
                " where rownum < " + id + " and OWNER like '" + schema + "'" +
                " and owner not in ('SYS', 'MDSYS' ,'WMSYS', 'SQLTXPLAIN', 'CTXSYS', 'EXFSYS', 'XDB')";

            rset = stmt.executeQuery(sqlText);

            LOGGER.debug(sqlText + "\n");
            //rset.next();
            while (rset.next()) {
                //xmlDocument = (rset).getLong("TEXT");
                data = rset.getBinaryStream("TEXT");
                sqlid = (rset).getString("VIEWS");
                //owner=(rset).getString("owner");
                chunkSize = 0;


                outputTextFile1 = new File(Filename + "_" + sqlid + ".txt");
                outputFileOutputStream = new FileOutputStream(outputTextFile1, true);


                try {
                    while ((chunkSize = data.read()) != -1)
                        outputFileOutputStream.write(chunkSize);
                } catch (Exception e) {
                    String err = e.toString();
                    System.out.println(err);
                }


                outputFileOutputStream.close();
            }


            rset.close();
            stmt.close();
            conn.commit();

            System.out.println("==========================================================\n" + "  CLOB read\n" +
                               "==========================================================\n");

            //return xmlDocument;


        } catch (SQLException e) {
            System.out.println("Caught SQL Exception: (Write CLOB value to file - Streams Method).");
            System.out.println("SQL:\n" + sqlText);
            e.printStackTrace();
            throw e;
        }

    }

    private int getAgeDifferenz(File outputTextFile1, File tmp_file) {

        int minutes = 0;

        TimeZone.setDefault(TimeZone.getDefault());

        try {

            if (!tmp_file.exists()) {
                tmp_file.createNewFile();
            }

            if (outputTextFile1.exists()) {

                Path filePath = outputTextFile1.toPath();


                BasicFileAttributes attributes = null;
                try {
                    attributes = Files.readAttributes(filePath, BasicFileAttributes.class);
                } catch (IOException exception) {
                    System.out.println("Exception handled when trying to get file " + "attributes: " +
                                       exception.getMessage());
                }


                Path filePath2 = tmp_file.toPath();

                BasicFileAttributes attributes2 = null;
                try {
                    attributes2 = Files.readAttributes(filePath2, BasicFileAttributes.class);
                } catch (IOException exception) {
                    System.out.println("Exception handled when trying to get file " + "attributes: " +
                                       exception.getMessage());
                }


                //TimeZone.getDefault() ;
                //TimeZone.getTimeZone("UTC")


                long milliseconds = attributes.lastModifiedTime().to(TimeUnit.MILLISECONDS);
                long milliseconds2 = attributes2.creationTime().to(TimeUnit.MILLISECONDS);
                //Date now = new Date();
                //long milli2 = now.getTime();

                if ((milliseconds > Long.MIN_VALUE) && (milliseconds < Long.MAX_VALUE)) {
                    minutes = Math.round((milliseconds - milliseconds2) / 1000 / 60);
                    LOGGER.debug("Compare " + filePath.toString() + " " + filePath2.toString() + " " +
                                 Math.round((milliseconds - milliseconds2) / 1000 / 60) + " Minutes old ");

                    LOGGER.debug("Creation time \n" + attributes.creationTime().toString() + " \n " +
                                 attributes2.creationTime().toString() + " \n " + milliseconds2 + " \n " +
                                 milliseconds);


                }

                //outputTextFile2.delete();
            }
        } catch (IOException e) {
            LOGGER.error("IO " + e);
        }

        return minutes;
    }


}
