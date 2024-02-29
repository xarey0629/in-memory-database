package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.LongValue;

import javax.swing.table.TableColumn;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Tuple {
    /**
     * A Tuple implements a hash table for storing its values.
     * Key: table + attribute
     * Value: Corresponding LongValue.
     */
    LinkedHashMap<String, LongValue> tuple;

    /**
     * Constructor for Tuple
     * @param table
     * @param attributes
     * @param values
     */
    Tuple(String table, String[] attributes, LongValue[] values){
        this.tuple = new LinkedHashMap<String, LongValue>();
        for(int i = 0; i < attributes.length; i++){
            String tableAttr = table + "." + attributes[i];
            tuple.put(tableAttr, values[i]);
        }
    }

    /**
     * Constructor for Project Operator.
     * @param columns
     * @param longValues
     */
    Tuple(String[] columns, LongValue[] longValues){
        this.tuple = new LinkedHashMap<String, LongValue>();
        for(int i = 0; i < columns.length; i++){
            this.tuple.put(columns[i], longValues[i]);
        }
    }

    Tuple(LinkedHashMap<String,LongValue> joinedTuple){
        this.tuple = joinedTuple;
    }

    public Tuple join(Tuple rightTuple){
        LinkedHashMap<String, LongValue> joinedTuple = new LinkedHashMap<String, LongValue>(this.tuple);
        for(String key:rightTuple.tuple.keySet()){
            LongValue longValue = rightTuple.tuple.get(key);
            joinedTuple.put(key, longValue);
//            System.out.printf("Join K/V: %s / %d \n", key, longValue.getValue());
        }
//        System.out.println(printTuple());
        return new Tuple(joinedTuple);
    }

    /**
     * Print the content of this tuple.
     * @return A csv-like string.
     */
    public String printTuple(){
        String output = "";
        for(String str: tuple.keySet()){
            output += (int)tuple.get(str).getValue() + ", ";
        }
        return output.substring(0, output.length() - 2);
    }

    /**
     * Project a new tuple for selection.
     * @param columns
     * @return a new tuple on selected attributes.
     */
    public Tuple projectTuple(String[] columns){
        LongValue[] longValues = new LongValue[columns.length];
        for(int i = 0; i < columns.length; i++){
            longValues[i] = this.tuple.get(columns[i]);
        }
        return new Tuple(columns, longValues);
    }
}
