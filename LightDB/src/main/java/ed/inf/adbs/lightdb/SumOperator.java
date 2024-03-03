package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.HashMap;

public class SumOperator extends Operator{
    DuplicateEliminationOperator duplicateEliminationOperator;
    String sumExpression;
    String[] groupByColumns;

    /**
     * Constructor for Sum Operator, whose child is duplicateEliminationOperator;
     * @param dbpath
     * @param schema
     * @param table
     * @param joinTables
     * @param columns
     * @param whereExpression
     * @param orderByColumns
     * @param isDistinct
     * @param groupByColumns
     */
    SumOperator(String dbpath, HashMap<String,String[]> schema, String table, String[] joinTables, String[] columns, Expression whereExpression, String[] orderByColumns, boolean isDistinct, String[] groupByColumns){
        this.duplicateEliminationOperator = new DuplicateEliminationOperator(dbpath, schema, table, joinTables, columns, whereExpression, orderByColumns, isDistinct);
        this.groupByColumns = groupByColumns;
    }

    // TODO: Implements methods getNextTuple, reset, dump and GROUP-BY.


}
