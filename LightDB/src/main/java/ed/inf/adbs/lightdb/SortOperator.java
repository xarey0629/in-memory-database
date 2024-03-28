package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

/**
 * Sort Operator which takes tuples from its child Project Operator and sort them.
 * This Operator relies on child Project Operator's functions and only used to sort.
 */
public class SortOperator extends Operator{
    ProjectOperator projectOperator;
    String[] orderByColumns;

    /**
     * Construct for Sort Operator which accepts the output from one of three types of Project Operators.
     * @param dbpath
     * @param schema
     * @param table
     * @param joinTables
     * @param columns
     * @param whereExpression
     * @param orderByColumns
     */
    SortOperator(String dbpath, HashMap<String,String[]> schema, String table, String[] joinTables, String[] columns, Expression whereExpression, String[] orderByColumns){
        this.orderByColumns = orderByColumns;
        if(joinTables != null) this.projectOperator = new ProjectOperator(dbpath, schema, table, joinTables, columns, whereExpression);
        else if(whereExpression == null)     this.projectOperator = new ProjectOperator(dbpath, schema, table, columns);
        else if(joinTables == null)     this.projectOperator = new ProjectOperator(dbpath, schema, table, columns, whereExpression);
        else                            this.projectOperator = new ProjectOperator(dbpath, schema, table, joinTables, columns, whereExpression);
    }

    // Blocking operation.
    @Override
    Tuple getNextTuple(){
//        System.out.println("SortOperator's getNextTuple doesn't have real meaning, please use its child Project Operator.");
        return null;
    }

    /**
     * Reset its child ProjectOperator.
     */
    @Override
    void reset(){
        this.projectOperator.reset();
    }

    /**
     * Use its child ProjectOperator.dump();
     * @return sortedTuples
     */
    @Override
    ArrayList<Tuple> dump() {
        ArrayList<Tuple> tuples = projectOperator.dump();
        if (orderByColumns == null) return tuples;
        else return sort(tuples);
    }

    /**
     * Sort Tuples from project operator by the order of SortBy columns;
     * Self-defined comparator: Compare each column lexicographically.
     * Get the values by hash map inside each tuple using column names as keys.
     * @param tuples
     * @return sorted tuples
     */
    public ArrayList<Tuple> sort(ArrayList<Tuple> tuples){
        if(tuples == null || orderByColumns == null) return null;
        ArrayList<Tuple> sortedTuples = new ArrayList<Tuple>(tuples);
        Collections.sort(sortedTuples, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple t1, Tuple t2){
                for(int i = 0; i < orderByColumns.length; i++) {
                    String column = orderByColumns[i];
                    if (t1.tuple.get(column).getValue() == t2.tuple.get(column).getValue()) continue;
                    return (int) (t1.tuple.get(column).getValue() - t2.tuple.get(column).getValue());
                }
                return 0;
            }
        });
        return sortedTuples;
    }
}
