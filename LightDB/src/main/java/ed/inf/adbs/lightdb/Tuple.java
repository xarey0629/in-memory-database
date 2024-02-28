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
            String tableAttr = table+attributes[i];
            tuple.put(tableAttr, values[i]);
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
