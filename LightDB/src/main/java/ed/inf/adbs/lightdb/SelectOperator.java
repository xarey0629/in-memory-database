package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * SelectOperator receives tuples from the child ScanOperator.
 * SQL keyword WHERE leads to the use of SelectOperator.
 */
public class SelectOperator extends Operator{
    HashMap<String,String[]> schema;
    ScanOperator scanOperator;
    Expression whereExpression;

    /**
     * Constructor for Select Operator
     * Accept where expression to filter as early as possible.
     * @param dbpath
     * @param schema
     * @param table
     * @param whereExpression
     */
    SelectOperator(String dbpath, HashMap<String,String[]> schema, String table, Expression whereExpression){
        this.schema = schema;
        this.scanOperator = new ScanOperator(dbpath, schema, table);
        this.whereExpression = whereExpression;
    }


    /**
     * Get a next verified tuple from Scan Operator.
     * @return a verified tuple or null if no.
     */
    @Override
    Tuple getNextTuple() {
        Tuple tuple = scanOperator.getNextTuple();
        while(tuple != null && !examineTuple(tuple, whereExpression)){
            tuple = scanOperator.getNextTuple();
        }
        return tuple;
    }

    @Override
    void reset(){
        scanOperator.reset();
    }

    /**
     * Utilized function getNextTuple() to get all verified tuples.
     * @return an ArrayList<Tuple> of all verified tuples.
     */
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
     * Examine a tuple whether it matches the whereExpression or not.
     * This utilizes the visitor, please see details in class MyExpressionDeParser.
     * @param tuple
     * @param whereExpression
     * @return true / false
     */
    public boolean examineTuple(Tuple tuple, Expression whereExpression){
        if(whereExpression == null) return true;
        MyExpressionDeParser myExpressionDeParser = new MyExpressionDeParser(tuple);
        StringBuilder stringBuilder = new StringBuilder();
        myExpressionDeParser.setBuffer(stringBuilder);
        whereExpression.accept(myExpressionDeParser);
        return myExpressionDeParser.examine();
    }

}
