package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;

public class ProjectOperator extends Operator{
    ScanOperator scanOperator;
    SelectOperator selectOperator;
    JoinOperator joinOperator;
    String[] columns;
    boolean isWhere;
    String[] whereColumns = null;
    boolean isJoin;

    /**
     * Constructor for Project Operator.
     * Child: Scan Operator because no WHERE clause.
     * @param dbpath
     * @param schema
     * @param table
     * @param columns
     */
    ProjectOperator(String dbpath, HashMap<String,String[]> schema, String table, String[] columns){
//        if(columns[0].equals("*")) this.columns = null;
        if(columns == null) this.columns = null;
        else this.columns = columns;
        this.scanOperator = new ScanOperator(dbpath, schema, table);
        this.isJoin = false;
        this.isWhere = false;
    }



    /**
     * Constructor for Project Operator.
     * Child: Select Operator because WHERE clause exists.
     * @param dbpath
     * @param schema
     * @param table
     * @param columns
     * @param whereExpression
     */
    ProjectOperator(String dbpath, HashMap<String,String[]> schema, String table, String[] columns, Expression whereExpression){
        if(columns == null) this.columns = null;
        else this.columns = columns;
        this.selectOperator = new SelectOperator(dbpath, schema, table, whereExpression);
        this.isJoin = false;
        this.isWhere = true;
        if(whereExpression != null){
            this.isWhere = true;
            this.whereColumns = getWhereColumns(whereExpression);
        }
    }

    /**
     * Constructor for Project Operator.
     * Child: Join Operator (to handle multiple tables).
     * @param dbpath
     * @param schema
     * @param table
     * @param joinTables
     * @param columns
     * @param whereExpression
     */
    ProjectOperator(String dbpath, HashMap<String,String[]> schema, String table, String[] joinTables, String[] columns, Expression whereExpression){
        if(columns == null) this.columns = null;
        else this.columns = columns;
        this.joinOperator = new JoinOperator(dbpath, schema, table, joinTables, whereExpression);
        this.isJoin = true;
        if(whereExpression != null){
            this.isWhere = true;
            this.whereColumns = getWhereColumns(whereExpression);
        }
    }

    @Override
    Tuple getNextTuple(){
        Tuple tuple;
        if(isJoin)       tuple = this.joinOperator.getNextTuple();
        else if(isWhere) tuple = this.selectOperator.getNextTuple();
        else             tuple = this.scanOperator.getNextTuple();

        if(tuple == null)          return null;
        else if(columns == null)   return tuple;
        else{
            boolean isLast = false;
            if(isJoin) isLast = true;
            return tuple.projectTuple(columns, whereColumns, isLast);
        }
    }

    @Override
    void reset(){
        if(isJoin)      joinOperator.reset();
        if(isWhere)     selectOperator.reset();
        else            scanOperator.reset();
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

    private String[] getWhereColumns(Expression whereExpression){
        String[] items = whereExpression.toString().split(" ");
        ArrayList<String> whereColumns = new ArrayList<String>();
        for(String item:items){
            if(item.contains(".")){
                whereColumns.add(item);
//                System.out.println("Find a whereColumn: " + item);
            }
        }
        return whereColumns.toArray(new String[0]);
    }

}
