package com.dbam4.dam;

import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.List;


public class TableDef {
    //public String ColumnName;
    //public int CSVColumnOrder;
    public String TableType;
    public String Type;
    //public String ColumnDescription;
    //public String ColumnValue;
    public String TableName;
    public String FILE_NAME;

    public String[] Aggregates;
    public String[] Keys;

    private List<ColDef> ListColumnDef;

    public List<ColDef> getListColumnDef() {
        return ListColumnDef;
    }

    public TableDef() {
        ListColumnDef = new ArrayList<ColDef>();
    }

    public void addColumnDef(ColDef ColumnDef) {
        this.ListColumnDef.add(ColumnDef);
    }

    public String getValueFromRowName(String RowName) {
        String value = "";
        for (ColDef item : ListColumnDef) {
            if (item.getColumnName().equalsIgnoreCase(RowName)) {
                value = item.getColumnValue();
            }

        }

        if (value.isEmpty()) {
            throw new EmptyStackException();
        }


        return value;

    }


    public String toString() {

        String output;
        output = "Table Definition for " + TableType + "\n";
        output = output + "Table " + TableName + " File " + FILE_NAME;
        //output = output + "Value: " + ColumnValue + "\n";

        return output;
    }

    public String toDetailString() {

        String output;
        output = " Type " + TableType + " Name " + TableName + " File " + FILE_NAME;
        //output = output + "Value: " + ColumnValue + "\n";

        for (ColDef item : ListColumnDef) {
            output = output + item.toString();
        }

        return output;
    }


}
