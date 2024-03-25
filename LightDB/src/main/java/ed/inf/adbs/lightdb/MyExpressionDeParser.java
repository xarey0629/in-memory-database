package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.LinkedList;
import java.util.Queue;

/**
 * This class extends ExpressionDeParser.
 * It extracts JOIN conditions and supports evaluating WHERE Expression and query optimizing by implementing two queues.
 * We assume WHERE clause is at most a conjunction(AND). Consequently, it returns false when ExpressionDeParser first detects a false predicate in a leaf whereExpression(A op B).
 * <p>
 * Mechanism:
 * 1. Utilize Visitor class ExpressionDeParser and override visit functions, which extract elements from WHERE clause in a DFS order.
 * 2. Store elements read from visitor functions into two queues, one for numeric elements another for booleans.
 * 3. Extract either values from the numeric queue if a column or a number is found
 * 4. Extract two numeric elements from the numeric queue and push the result into the boolean queue if a comparison operator is found.
 * <p>
 * Note: More details written in comments beyond methods.
 */
public class MyExpressionDeParser extends ExpressionDeParser {
    final Queue<Long> numericQueue = new LinkedList<Long>();
    final Queue<Boolean> boolQueue = new LinkedList<Boolean>();
    final long flag = Long.MIN_VALUE;   // Flag used to identify finding a column doesn't belong to this join subtree.
    Tuple tuple;
    Tuple rightTuple;
    String[] leftTables;
    String rightTable;
    Boolean isJoin;

    /**
     * Constructor for Select Operator.
     * @param tuple
     */
    MyExpressionDeParser(Tuple tuple) {
        this.isJoin = false;
        this.tuple = tuple;
    }

    /**
     * Constructor for Join Operator
     * @param leftTuple
     * @param rightTuple
     * @param leftTables
     * @param rightTableName
     */
    MyExpressionDeParser(Tuple leftTuple, Tuple rightTuple, String[] leftTables, String rightTableName) {
        this.isJoin = true;
        this.tuple = leftTuple;
        this.rightTuple = rightTuple;
        this.leftTables = leftTables;
        this.rightTable = rightTableName;
    }

    /**
     * The following methods traverse whereExpression and then de-parse it by using visitor.
     * When a numeric leaf node is reached, it pushes its value into the numeric queue,
     * When a comparison operator leaf node is reached, it polls values from the numeric queue and push the result into the boolean queue.
     * For attributes handling, please see function visit(Column).
     *
     * Note: All visiting functions is optimized to support complex predicates breaking.
     * @param andExpression
     */
    @Override
    public void visit(AndExpression andExpression) {
//        if(predicatesOptimization()) return;
//        System.out.println("Visit AndExpression: " + andExpression.toString());
        super.visit(andExpression);
        if (predicatesOptimization()) return;
        boolean expr1 = boolQueue.poll();
        boolean expr2 = boolQueue.poll();
        boolQueue.offer(expr1 && expr2);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        if (predicatesOptimization()) return;
//        System.out.println("Visit EqualsTo: " + equalsTo.toString());
        super.visit(equalsTo);
        if (predicatesOptimization()) return;

        long lvalue = numericQueue.poll();
        long rvalue = numericQueue.poll();

        if (lvalue == flag || rvalue == flag) boolQueue.offer(true);
        else boolQueue.offer(lvalue == rvalue);

    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
//        System.out.println("Visit NotEqualsTo: " + notEqualsTo.toString());
        if (predicatesOptimization()) return;
        super.visit(notEqualsTo);
        if (predicatesOptimization()) return;

        long lvalue = numericQueue.poll();
        long rvalue = numericQueue.poll();

        if (lvalue == flag || rvalue == flag) boolQueue.offer(true);
        else boolQueue.offer(lvalue != rvalue);

    }

    @Override
    public void visit(GreaterThan greaterThan) {
//        System.out.println("Visit GreaterThan: " + greaterThan.toString());
        if (predicatesOptimization()) return;
        super.visit(greaterThan);
        if (predicatesOptimization()) return;

        long lvalue = numericQueue.poll();
        long rvalue = numericQueue.poll();

        if (lvalue == flag || rvalue == flag) boolQueue.offer(true);
        else boolQueue.offer(lvalue > rvalue);

    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
//        System.out.println("Visit GreaterThanEquals: " + greaterThanEquals.toString());
        if (predicatesOptimization()) return;
        super.visit(greaterThanEquals);
        if (predicatesOptimization()) return;

        long lvalue = numericQueue.poll();
        long rvalue = numericQueue.poll();

        if (lvalue == flag || rvalue == flag) boolQueue.offer(true);
        else boolQueue.offer(lvalue >= rvalue);


    }

    @Override
    public void visit(MinorThan minorThan) {
//        System.out.println("Visit MinorThan: " + minorThan.toString());
        if (predicatesOptimization()) return;
        super.visit(minorThan);
        if (predicatesOptimization()) return;

        long lvalue = numericQueue.poll();
        long rvalue = numericQueue.poll();

        if (lvalue == flag || rvalue == flag) boolQueue.offer(true);
        else boolQueue.offer(lvalue < rvalue);

    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        if (predicatesOptimization()) return;
//        System.out.println("Visit MinorThanEquals: " + minorThanEquals.toString());
        super.visit(minorThanEquals);
        if (predicatesOptimization()) return;

        long lvalue = numericQueue.poll();
        long rvalue = numericQueue.poll();

        if (lvalue == flag || rvalue == flag) boolQueue.offer(true);
        else boolQueue.offer(lvalue <= rvalue);

    }

    @Override
    public void visit(LongValue longValue) {
//        System.out.println("Visit LongValue: " + longValue.toString());
        super.visit(longValue);
        numericQueue.offer(longValue.getValue());
    }

    /**
     * If it's column under a Join Operator, we should check whether the current verifying column belongs to this subtree or not.
     * If not, we raise a flag and assume it is true by pushing a MIN_VALUE into the queue.
     * If yes, we can verify it.
     * @param column
     */
    @Override
    public void visit(Column column) {
//        System.out.println("Visit Column: " + column.toString());
        if (predicatesOptimization()) return;
        super.visit(column);
        if (predicatesOptimization()) return;

        // Join Conditions
        String key = column.toString();
        if (isJoin) {
            if (rightTuple.tuple.containsKey(key)) numericQueue.offer(rightTuple.tuple.get(key).getValue());
            else if (tuple.tuple.containsKey(key)) numericQueue.offer(tuple.tuple.get(key).getValue());
            else
                numericQueue.offer(flag); // This raises a flag if the current column doesn't belong to this tree level.
        } else {
            if (tuple.tuple.containsKey(key)) numericQueue.offer(this.tuple.tuple.get(key).getValue());
            else
                numericQueue.offer(flag); // This raises a flag if the current column doesn't belong to this tree level.
        }
    }

    /**
     * Examine an expression by polling a boolean from queue.
     *
     * @return true if verified.
     */
    public boolean examine() {
        return boolQueue.poll();
    }

    /**
     * Detect any false condition in predicate to optimize selection.
     *
     * @return ture if a predicate is found false (we only have conjunction).
     */
    private boolean predicatesOptimization() {
        // Predicate optimization: Break complex predicates and push them down.
        if (boolQueue.contains(false)) {
//            System.out.println("Find false in early stage, return.");
            boolQueue.clear();
            boolQueue.offer(false);
            return true;
        } else return false;
    }
}
