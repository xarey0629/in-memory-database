package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;

public class DuplicateEliminationOperator extends Operator{
    SortOperator sortOperator;
    boolean isDistinct;

    DuplicateEliminationOperator(String dbpath, HashMap<String,String[]> schema, String table, String[] joinTables, String[] columns, Expression whereExpression, String[] orderByColumns, boolean isDistinct){
        this.sortOperator = new SortOperator(dbpath, schema, table, joinTables, columns, whereExpression, orderByColumns);
        this.isDistinct = isDistinct;
    }

    @Override
    Tuple getNextTuple(){
        System.out.println("DuplicateEliminationOperator's getNextTuple doesn't have real meaning, please use its child Project Operator.");
        return null;
    }

    @Override
    void reset(){
        this.sortOperator.reset();
    }

    @Override
    ArrayList<Tuple> dump() {
        ArrayList<Tuple> tuples = sortOperator.dump();
        if (this.isDistinct) return eliminateDuplicates(tuples);
        else return tuples;
    }

    public ArrayList<Tuple> eliminateDuplicates(ArrayList<Tuple> sortedTuples){
        if(sortedTuples == null) return null;
        // TODO: Determine whether two tuples are different or not.
        ArrayList<Tuple> uniqueTuples = new ArrayList<Tuple>();
        Tuple currTuple = sortedTuples.get(0);
        for(int i = 1; i < sortedTuples.size(); i++){
            if(!currTuple.equalsTo(sortedTuples.get(i))){
                uniqueTuples.add(currTuple);
                currTuple = sortedTuples.get(i);
            }
        }
        uniqueTuples.add(currTuple);
        return uniqueTuples;
    }


}
