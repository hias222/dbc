package ise.dbc;

import oracle.net.jdbc.TNSAddress.Description;

import org.apache.logging.log4j.core.config.Order;

public class ColDef implements java.io.Serializable {
    @SuppressWarnings("compatibility:-6043852885703426182")
    private static final long serialVersionUID = 1L;

    private String ColumnDescription;
    private String ColumnType;

    private String ColumnName;
    private int CSVColumnOrder;

    private String ColumnValue;


    public String getColumnDescription() {
        return ColumnDescription;
    }

    public String getColumnType() {
        return ColumnType;
    }

    public String getColumnName() {
        return ColumnName;
    }

    public int getCSVColumnOrder() {
        return CSVColumnOrder;
    }


    public void setColumnValue(String ColumnValue) {
        this.ColumnValue = ColumnValue;
    }

    public String getColumnValue() {
        return ColumnValue;
    }

    public ColDef(String ColumnName) {
        this.ColumnName = ColumnName;
    }

    public void addCollTyps(int CSVColumnOrder, String ColumnType, String ColumnDescription) {
        this.CSVColumnOrder = CSVColumnOrder;
        this.ColumnType = ColumnType;
        this.ColumnDescription = ColumnDescription;
    }
    
    public String toString(){
        String output = ("Name " + ColumnName + " Order " + CSVColumnOrder + " Type " + ColumnType + " Description " + ColumnDescription + " Value " + ColumnValue);
        return output;
    }


}
