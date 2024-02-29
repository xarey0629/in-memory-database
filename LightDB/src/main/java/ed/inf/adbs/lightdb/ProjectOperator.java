package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;

public class ProjectOperator extends Operator{
    ScanOperator scanOperator;
    SelectOperator selectOperator;
    String[] columns;
    boolean isWhere;
    boolean isJoin;

    /**
     * Constructor for ScanOperator.
     * @param dbpath
     * @param schema
     * @param table
     * @param columns
     */
    ProjectOperator(String dbpath, HashMap<String,String[]> schema, String table, String[] columns){
        this.columns = columns;
        this.scanOperator = new ScanOperator(dbpath, schema, table);
        this.isJoin = false;
        this.isWhere = false;
    }

    /**
     * Constructor for SelectOperator.
     * @param dbpath
     * @param schema
     * @param table
     * @param columns
     * @param whereExpression
     */
    ProjectOperator(String dbpath, HashMap<String,String[]> schema, String table, String[] columns, Expression whereExpression){
        this.columns = columns;
        this.selectOperator = new SelectOperator(dbpath, schema, table, whereExpression);
        this.isJoin = false;
        this.isWhere = true;
    }

    @Override
    Tuple getNextTuple(){
        Tuple tuple;
        if(isWhere){
            tuple = selectOperator.getNextTuple();
        }else{
            tuple = scanOperator.getNextTuple();
        }
        
        if(tuple == null) return null;

        if(this.columns == null){
            return tuple;
        }else{
            return tuple.projectTuple(columns);
        }
    }

    @Override
    void reset(){
        if(isWhere) selectOperator.reset();
        else scanOperator.reset();
    }

    @Override
    ArrayList<Tuple> dump() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Tuple tuple = getNextTuple();
        while(tuple != null){
            tuples.add(tuple);
            tuple = getNextTuple();
        }
        return tuples;
    }



}
