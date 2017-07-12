package ise.dbc;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import java.io.IOException;

import java.io.OutputStreamWriter;

import java.io.PrintWriter;

import java.nio.charset.Charset;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Types;

import java.text.Normalizer;
import java.text.SimpleDateFormat;

import java.util.Date;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import oracle.jdbc.OracleTypes;

import oracle.sql.CHAR;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.XMLConfigurationFactory;

import sun.misc.IOUtils;

public class getSessionData implements Runnable {

    private Object OracleConnection;

    static {
        System.setProperty(XMLConfigurationFactory.CONFIGURATION_FILE_PROPERTY, "log4j2.xml");
    }

    private static final Logger LOGGER = LogManager.getLogger(ise.dbc.getSessionData.class);


    private Connection conn = null;

    CallableStatement cs;
    ResultSet cursorResultSet;

    String sqlText = null;
    String FileDir = "/tmp";
    String FileName = "/tmp/tmp.txt";
    Statement stmt = null;
    ResultSet rset = null;
    String PLSQLProcedureFile = "/tmp/dbc_sessions_java.sql";
    Integer Runs = 100;
    Integer Waits = 100;


    String plsql;

    String sqlid;
    File outputTextFile1 = null;
    File tmp_file = null;
    FileOutputStream outputFileOutputStream;
    OutputStreamWriter outputOutputStreamWriter;
    BufferedWriter outputBufferedWriter;
    Integer pollingMinutes;


    public getSessionData(Object OracleConnection, String FileDir, String SessionScript, Integer Runs, Integer Waits, Integer PollingMinutes) {
        this.OracleConnection = OracleConnection;
        this.FileDir = FileDir;
        this.FileName = FileDir + "/actual_data.txt";
        this.PLSQLProcedureFile = SessionScript;
        this.Runs = Runs;
        this.Waits = Waits;
        this.pollingMinutes = PollingMinutes;        
       
        Charset charset = Charset.defaultCharset();
        //charset = StandardCharsets.UTF_16LE;
           
           LOGGER.info("Default encoding: " + charset + " (Aliases: "
                + charset.aliases() + ")");


    }


    public void run() {

        int numrows = 0;

      //  String[] searchList = { "�", "�", "�", "�", "�", "�", "�" };
      //  String[] replaceList = { "Ae", "ae", "Oe", "oe", "Ue", "ue", "sz" };
        
      Charset charset = Charset.defaultCharset();
      
       // charset = StandardCharsets.UTF_16LE;
      //charset =StandardCharsets.ISO_8859_1;
         


        try {


            outputTextFile1 = new File(this.FileName);
            tmp_file = new File(this.FileDir + "/.tmp_time");

            int fileSizeMB = (int) getFileSizeMB(outputTextFile1);
            LOGGER.debug("FILESIZE MB " + fileSizeMB);

            if (fileSizeMB > 10) {
                LOGGER.info("Should move it size " + this.FileName);
                SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMddHHmmss"); //dd/MM/yyyy
                Date now = new Date();
                String strDate = sdfDate.format(now);
                LOGGER.debug("new Name is " + this.FileDir + "/SES_" + strDate + ".txt");

                File outputTextFile2 = new File(this.FileDir + "/SES_" + strDate + ".txt");
                if (outputTextFile1.exists()) {
                    Files.move(outputTextFile1.toPath(), outputTextFile2.toPath(), StandardCopyOption.ATOMIC_MOVE);
                }


                outputTextFile1 = new File(this.FileName);

            } else {
                //creation date
                if (getCreationDate(outputTextFile1, tmp_file) > pollingMinutes) {
                    
                    LOGGER.info("Should move it time " + this.FileName);
                    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMddHHmmss"); //dd/MM/yyyy
                    Date now = new Date();
                    String strDate = sdfDate.format(now);
                    LOGGER.debug("new Name is " + this.FileDir + "/SES_" + strDate + ".txt");

                    File outputTextFile2 = new File(this.FileDir + "/SES_" + strDate + ".txt");

                    if (outputTextFile1.exists()) {
                        Files.move(outputTextFile1.toPath(), outputTextFile2.toPath(), StandardCopyOption.ATOMIC_MOVE);
                    }

                    outputTextFile1 = new File(this.FileName);
                    tmp_file.delete();


                }
            }


            conn = (Connection) OracleConnection;
            
         
            outputFileOutputStream = new FileOutputStream(outputTextFile1,true);
            //outputFileOutputStream = new FileOutputStream(outputTextFile1,"UTF-8");
            //Charset.forName("UTF-8").newEncoder());

            outputOutputStreamWriter =
                //new OutputStreamWriter(outputFileOutputStream, Charset.forName("UTF-8").newEncoder());
                new OutputStreamWriter(outputFileOutputStream, charset);
            
     
            LOGGER.debug("Output to = " + outputTextFile1); //+ "\n" + this.PLSQLProcedureFile);
            LOGGER.debug("Runs " + Runs + " Waits " + Waits);
            
            //PrintWriter pw  =  new PrintWriter(outputFileOutputStream,true);
            outputBufferedWriter = new BufferedWriter(outputOutputStreamWriter);


            cs = conn.prepareCall(this.PLSQLProcedureFile);

            cs.setInt(1, Runs);
            cs.setInt(2, Waits);

            cs.registerOutParameter(3, OracleTypes.CURSOR);

            cs.execute();

            cursorResultSet = (ResultSet) cs.getObject(3);
            
            //PrintWriter pw  =  new PrintWriter(new FileOutputStream("test_cursor.txt",true));
            
            
           
            
            while (cursorResultSet.next()) {
                
                String newoutput = cursorResultSet.getString(1);
   
              

                StringBuilder builder = new StringBuilder();

                for (char currentChar : newoutput.toCharArray()) {
                    Character replacementChar = null;
                    if (Character.UnicodeBlock.of(currentChar) != Character.UnicodeBlock.BASIC_LATIN) {
                     // replace with Y
                     replacementChar = '_';
                     LOGGER.info("++++ Found " + currentChar );
                    }
                    
                    builder.append(replacementChar != null ? replacementChar : currentChar);
                }

                String newString = builder.toString();
             
                outputBufferedWriter.write(newString);
                outputBufferedWriter.write("\n");
                //pw.println("\n");
                numrows++;
            }
            
            cs.close();
            cursorResultSet.close();
            //outputBufferedWriter.write("ENDE ���� ���� ENDE \n");
            //pw.close();

            outputBufferedWriter.close();
            outputOutputStreamWriter.close();
            outputFileOutputStream.close();
           
          
//            conn.commit();

            LOGGER.info("Exported to " + this.FileName + " rows " + numrows);

        } catch (SQLException e) {
            e.printStackTrace(System.out);
            LOGGER.error("1 " + e);
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
            LOGGER.error("2 " +e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            e.printStackTrace(System.out);
            LOGGER.error("3 " +e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            e.printStackTrace(System.out);
            LOGGER.error("EX " +e.toString());
            throw new RuntimeException(e);
        }


    }
    
    public static String removeDiacriticalMarks(String string) {
        return Normalizer.normalize(string, Normalizer.Form.NFD)
            .replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
    }

    private long getFileSizeMB(File TextFile) {

        long space = 0;
        TextFile = new File(FileDir);
        if (TextFile.exists()) {
            space = TextFile.length();
            //long space = TextFile.getTotalSpace();

            space = space / 1024 / 1024;

            LOGGER.debug("SIZE " + space);
        }

        return space;
    }

    private int getCreationDate(File outputTextFile1, File tmp_file) {

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
            LOGGER.error(e + e.getMessage());
        }

        return minutes;
    }


}
