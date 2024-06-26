package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * The Join Operator Class implements a left-deep join tree.
 * Please see comments in methods for more information.
 * For extracting JOIN conditions, please see comments of the last method.
 */
public class JoinOperator extends Operator{
    String[] leftJoinTableNames;
    String rightTableName;
    Expression whereExpression;
    JoinOperator leftJoinOperator;
    SelectOperator leftSelectOperator;
    SelectOperator rightSelectOperator;
    Boolean isJoinTreeBottom = false;
    Tuple leftTuple = null;
    Tuple rightTuple = null;

    /**
     * Constructor for Join Operator
     * if(more than two tables){
     *      Create a left join tree.
     * }else{
     *     Create left and right Select Operators then combine.
     * }
     * @param dbpath
     * @param schema
     * @param table
     * @param leftJoinTables
     * @param whereExpression
     */
    JoinOperator(String dbpath, HashMap<String,String[]> schema, String table, String[] leftJoinTables, Expression whereExpression){
        this.leftJoinTableNames = leftJoinTables;
        this.rightTableName = table;
        this.whereExpression = whereExpression;
        if(leftJoinTableNames.length > 1){      // More than two tables -> Traverse left join trees.
            String[] leftChildJoinTables = Arrays.copyOfRange(leftJoinTables, 0, leftJoinTables.length - 1);
            // Create a left chill join operator.
            this.leftJoinOperator = new JoinOperator(dbpath, schema, table, leftChildJoinTables, whereExpression);
        }else{                                  // Two tables remain -> Combine
            this.leftSelectOperator = new SelectOperator(dbpath, schema, table, whereExpression);
            this.isJoinTreeBottom = true;
        }
        // Creat right leaf node table.
        this.rightSelectOperator = new SelectOperator(dbpath, schema, leftJoinTables[leftJoinTables.length - 1], whereExpression);
    }

    @Override
    Tuple getNextTuple(){
        Operator leftOperator;
        // Determine left child operator is select or join.
        if(isJoinTreeBottom) leftOperator = leftSelectOperator;
        else leftOperator = leftJoinOperator;
        // Get the correct outer tuple and inner tuple.
        if(leftTuple == null) leftTuple = leftOperator.getNextTuple();
        rightTuple = rightSelectOperator.getNextTuple();
        // Inner Loop reaches the end.
        if(rightTuple == null){
            rightSelectOperator.reset();
            rightTuple = rightSelectOperator.getNextTuple();
            // The previous left tuple finished. It's single nested loop join completely. So get the next left tuple.
            leftTuple = leftOperator.getNextTuple();
        }
        // Logic to evaluate conditions.
        while(leftTuple != null && !examineTuples(leftTuple, rightTuple, leftJoinTableNames, rightTableName, whereExpression)){
            rightTuple = rightSelectOperator.getNextTuple();
            while(rightTuple != null && !examineTuples(leftTuple, rightTuple, leftJoinTableNames, rightTableName, whereExpression)){
                rightTuple = rightSelectOperator.getNextTuple();
            }
            if(rightTuple == null){
                rightSelectOperator.reset();
                rightTuple = rightSelectOperator.getNextTuple();
                leftTuple = leftOperator.getNextTuple();
            }
        }
//        if(leftTuple != null) System.out.println("Left tuple: " + leftTuple.printTuple() + ", Right tuple: " + rightTuple.printTuple());
        if(leftTuple == null) return null;
        else return leftTuple.join(rightTuple);
    }

    @Override
    void reset(){
        leftTuple = rightTuple = null;
        rightSelectOperator.reset();
        if(isJoinTreeBottom) leftSelectOperator.reset();
        else leftJoinOperator.reset();
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
     * Examine left and right tuples are under the condition or not.
     * JOIN conditions are extracted by class MyExpressionDeParser.
     * @param leftTuple outer tuple
     * @param rightTuple inner tuple
     * @param leftTables tables in left tree
     * @param rightTable the single table as a right child node
     * @param whereExpression
     * @return true if a tuple is satisfied
     */
    public boolean examineTuples(Tuple leftTuple, Tuple rightTuple, String[] leftTables, String rightTable, Expression whereExpression){
        if(whereExpression == null) return true;
        // Extract JOIN conditions by MyExpressionDeParser.
        MyExpressionDeParser myExpressionDeParser = new MyExpressionDeParser(leftTuple, rightTuple, leftTables, rightTable);
        StringBuilder stringBuilder = new StringBuilder();
        myExpressionDeParser.setBuffer(stringBuilder);
        whereExpression.accept(myExpressionDeParser);
        return myExpressionDeParser.examine();
    }
}
