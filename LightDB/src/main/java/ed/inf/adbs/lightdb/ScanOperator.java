package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.LongValue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class ScanOperator extends Operator{
    String dbPath;
    String tableName;
    String tablePath;
    HashMap<String, String[]> schema;
    FileReader fileReader;
    BufferedReader bufferedReader;
    String currLine;

    /**
     * Constructor for Scan Operator.
     * @param dbpath
     * @param schema
     * @param table (hasAlias ? alias : table)
     */
    ScanOperator(String dbpath, HashMap<String,String[]> schema, String table) {
        this.dbPath = dbpath;
        this.schema = schema;
        this.tableName = table;

        // Read table
        if(SQLInterpreter.hasAlias) this.tablePath = this.dbPath + "/data/" + SQLInterpreter.aliasToTable.get(table) + ".csv";
        else this.tablePath = this.dbPath + "/data/" + table + ".csv";

        // Print
        System.out.printf(
                "Read Table: " +
                "Table %s, hasAlias: %s, path: %s \n", table, SQLInterpreter.hasAlias ? "True" : "False", this.tablePath
        );
        reset();
    }

    @Override
    Tuple getNextTuple() {
        if(currLine != null){
            String[] columnNames = schema.get(tableName);
            String[] data = currLine.split(",");
            for(int i = 0; i < data.length; i++){
                data[i] = data[i].trim();
            }
            LongValue[] values = new LongValue[columnNames.length];
            for (int i = 0; i < columnNames.length; i++){
                values[i] = new LongValue(Integer.parseInt(data[i])); // All attributes are integers.
            }

            Tuple t = new Tuple(tableName, columnNames, values);
            currLine = getNextLine();
            return t;
        }
        else{
            return null;
        }
    }
    @Override
    void reset(){
        try {
            fileReader = new FileReader(tablePath);
            bufferedReader = new BufferedReader(fileReader);
            this.currLine = getNextLine();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    ArrayList<Tuple> dump(){
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Tuple tuple = getNextTuple();
        while(tuple != null){
            tuples.add(tuple);
            tuple = getNextTuple();
        }
        return tuples;
    }

    /**
     * Read next line in the file, return null if at the EOF.
     * @return
     */
    String getNextLine(){
        String line = null;
        try{
            line = bufferedReader.readLine();
        }catch (IOException ioE){
            line = null;
        }
        return line;
    }
}
