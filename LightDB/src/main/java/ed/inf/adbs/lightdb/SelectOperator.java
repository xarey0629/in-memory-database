package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * SelectOperator receives tuples from ScanOperator.
 * SQL keyword WHERE leads to the use of SelectOperator.
 */
public class SelectOperator extends Operator{
    HashMap<String,String[]> schema;
    ScanOperator scanOperator;
    Expression whereExpression;

    SelectOperator(String dbpath, HashMap<String,String[]> schema, String table, Expression whereExpression){
        this.schema = schema;
        this.scanOperator = new ScanOperator(dbpath, schema, table);
        this.whereExpression = whereExpression;
    }

    @Override
    Tuple getNextTuple() {
        Tuple tuple = scanOperator.getNextTuple();
        while(tuple != null && !examineTuple(tuple, whereExpression)){
            tuple = scanOperator.getNextTuple();
        };
        return tuple;
    }

    @Override
    void reset(){
        scanOperator.reset();
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



    public boolean examineTuple(Tuple tuple, Expression whereExpression){
        MyExpressionDeParser myExpressionDeParser = new MyExpressionDeParser(tuple);
        StringBuilder stringBuilder = new StringBuilder();
        myExpressionDeParser.setBuffer(stringBuilder);
        whereExpression.accept(myExpressionDeParser);
        return myExpressionDeParser.examine();
    }

}
