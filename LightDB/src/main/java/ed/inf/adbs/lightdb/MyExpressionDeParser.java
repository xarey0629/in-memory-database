package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.LinkedList;
import java.util.Queue;

public class MyExpressionDeParser extends ExpressionDeParser {
    final Queue<Long> queueValue = new LinkedList<Long>();
    final Queue<Boolean> queueBoolean = new LinkedList<Boolean>();
    Tuple tuple;
    Tuple rightTuple;
    String[] leftTables;
    String rightTable;
    Boolean isJoin;

    /**
     * Constructor for Select Operator.
     * @param tuple
     */
    MyExpressionDeParser(Tuple tuple){
        this.isJoin = false;
        this.tuple = tuple;
    }

    /**
     * Constructor for Join Operator
     * @param leftTuple
     * @param rightTuple
     * @param leftTables
     * @param rightTable
     */
    MyExpressionDeParser(Tuple leftTuple, Tuple rightTuple, String[] leftTables, String rightTable){
        this.isJoin = true;
        this.tuple = leftTuple;
        this.rightTuple = rightTuple;
        this.leftTables = leftTables;
        this.rightTable = rightTable;
    }

    @Override
    public void visit(AndExpression andExpression){
        System.out.println("Visit AndExpression: " + andExpression.toString());

        super.visit(andExpression);
        boolean expr1 = queueBoolean.poll();
        boolean expr2 = queueBoolean.poll();
        queueBoolean.offer(expr1 && expr2);
    }

    @Override
    public void visit(EqualsTo equalsTo){
        System.out.println("Visit EqualsTo: " + equalsTo.toString());
        super.visit(equalsTo);

        long lvalue = queueValue.poll();
        long rvalue = queueValue.poll();


        if (lvalue == Long.MIN_VALUE || rvalue == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(lvalue == rvalue);
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo){
        System.out.println("Visit NotEqualsTo: " + notEqualsTo.toString());
        super.visit(notEqualsTo);

        long lvalue = queueValue.poll();
        long rvalue = queueValue.poll();
        if (lvalue == Long.MIN_VALUE || rvalue == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(lvalue != rvalue);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan){
        System.out.println("Visit GreaterThan: " + greaterThan.toString());
        super.visit(greaterThan);

        long lvalue = queueValue.poll();
        long rvalue = queueValue.poll();

        if (lvalue == Long.MIN_VALUE || rvalue == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(lvalue > rvalue);
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals){
        System.out.println("Visit GreaterThanEquals: " + greaterThanEquals.toString());

        super.visit(greaterThanEquals);

        long lvalue = queueValue.poll();
        long rvalue = queueValue.poll();

        if (lvalue == Long.MIN_VALUE || rvalue == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(lvalue >= rvalue);
        }

    }

    @Override
    public void visit(MinorThan minorThan){
        System.out.println("Visit MinorThan: " + minorThan.toString());
        super.visit(minorThan);

        long lvalue = queueValue.poll();
        long rvalue = queueValue.poll();

        if (lvalue == Long.MIN_VALUE || rvalue == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(lvalue < rvalue);
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals){
        System.out.println("Visit MinorThanEquals: " + minorThanEquals.toString());

        super.visit(minorThanEquals);

        long lvalue = queueValue.poll();
        long rvalue = queueValue.poll();

        if (lvalue == Long.MIN_VALUE || rvalue == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(lvalue <= rvalue);
        }
    }

    @Override
    public void visit(LongValue longValue){
        System.out.println("Visit LongValue: " + longValue.toString());
        super.visit(longValue);
        queueValue.offer(longValue.getValue());
    }

    @Override
    public void visit(Column column){
        System.out.println("Visit Column: " + column.toString());
        super.visit(column);
//        String table = column.getTable().getName();
//        String col = column.getColumnName();
        // TODO: Join Conditions
        String key = column.toString();
        if(isJoin){
            if(rightTuple.tuple.containsKey(key)) queueValue.offer(rightTuple.tuple.get(key).getValue());
            else if(tuple.tuple.containsKey(key)) queueValue.offer(tuple.tuple.get(key).getValue());
            else queueValue.offer(Long.MIN_VALUE);
        }else{
            if(tuple.tuple.containsKey(key)) queueValue.offer(this.tuple.tuple.get(key).getValue());
            else queueValue.offer(Long.MIN_VALUE);
        }


    }

    /**
     * Examine an expression.
     * @return true if verified.
     */
    public boolean examine(){
        return queueBoolean.poll();
    }
}
