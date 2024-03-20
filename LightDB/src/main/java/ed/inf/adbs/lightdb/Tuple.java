package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Tuple {
    /**
     * A Tuple implements a hash table for storing its values.
     * Key: "table.attribute"
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
     * @param columns: e.g. "Sailors.A", "S.B"
     * @param longValues
     */
    Tuple(String[] columns, LongValue[] longValues){
        this.tuple = new LinkedHashMap<String, LongValue>();
        for(int i = 0; i < columns.length; i++){
            this.tuple.put(columns[i], longValues[i]);
        }
    }

    /**
     * Constructor for receiving a linked hash map.
     * @param joinedTuple
     */
    Tuple(LinkedHashMap<String,LongValue> joinedTuple){
        this.tuple = joinedTuple;
        System.out.println("After join: " + printTuple() + "\n");
    }

    // ---------------------- Methods start from here. ----------------------

    /**
     * Check two tuples are equal or not.
     * @param t2
     * @return boolean
     */
    public boolean equalsTo(Tuple t2){
        if(this.tuple.keySet().equals(t2.tuple.keySet())){
            for(String key: this.tuple.keySet()){
                if(!this.tuple.get(key).equals(t2.tuple.get(key))) return false;
            }
        }else return false;
        return true;
    }

    /**
     * Create a new Tuple by joining two tuples.
     * @param rightTuple
     * @return
     */
    public Tuple join(Tuple rightTuple){
        LinkedHashMap<String, LongValue> joinedTuple = new LinkedHashMap<String, LongValue>(this.tuple);
        for(String key:rightTuple.tuple.keySet()){
            LongValue longValue = rightTuple.tuple.get(key);
            joinedTuple.put(key, longValue);
            System.out.printf("Join K/V: %s / %d \n", key, longValue.getValue());
        }
        return new Tuple(joinedTuple);
    }

    /**
     * Project a new tuple for SELECT clause.
     * @param columns: e.g. "Sailors.A", "S.B"
     * @return a new tuple on selected attributes.
     */
    public Tuple projectTuple(String[] columns, String[] whereColumns, boolean isLast){
        if(isLast || whereColumns == null){
            LongValue[] longValues = new LongValue[columns.length];
            for(int i = 0; i < columns.length; i++){
                longValues[i] = this.tuple.get(columns[i]);
            }
            return new Tuple(columns, longValues);
        }
        else{
            ArrayList<String> newColumns = new ArrayList<String>();
            ArrayList<LongValue> longValues = new ArrayList<LongValue>();
            for (String column : columns) {
                newColumns.add(column);
                longValues.add(this.tuple.get(column));
            }
            for (String whereColumn : whereColumns) {
                if (tuple.containsKey(whereColumn)) {
                    newColumns.add(whereColumn);
                    longValues.add(this.tuple.get(whereColumn));
                }
            }
            return new Tuple(newColumns.toArray(new String[0]), longValues.toArray(new LongValue[0]) );
        }
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
}
