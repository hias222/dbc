package ise.dbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleResultSet;

import oracle.sql.CLOB;

public class putClob {

    CLOB xmlDocument;

    public void writeCLOBPut(CLOB newxmlDocument, Connection conn,
                             Number id) throws IOException, SQLException {


        String sqlText = null;
        Statement stmt = null;
        ResultSet rset = null;

        try {

            stmt = conn.createStatement();

            PreparedStatement ps =
                conn.prepareStatement("INSERT INTO test_clob (id, document_name, xml_document, timestamp) " +
                                      "VALUES(" + id +
                                      " , 'testin' , ? , sysdate)");
            //("INSERT INTO raw_table VALUES(?)");
            ((OraclePreparedStatement)ps).setCLOB(1, newxmlDocument);
            ps.execute();

            conn.commit();
//            rset.close();
            stmt.close();

            System.out.println("==========================================================\n" +
                    "  PUT CLOB \n" +
                    "==========================================================\n");


        } catch (SQLException e) {
            System.out.println("Caught SQL Exception: (Write CLOB value - Put Method).");
            System.out.println("SQL:\n" +
                    sqlText);
            e.printStackTrace();
            throw e;
        }

    }
}
