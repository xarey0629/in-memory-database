package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class SumOperator extends Operator{
    DuplicateEliminationOperator duplicateEliminationOperator;
    boolean isGroupBy = false;
    String[] groupByColumns;
    boolean isSum = false;
    String sumExpression;
    HashMap<String, Long> sumHashMap;
    HashMap<String, Tuple> tupleHashMap;
    String[] selectItems;

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
    SumOperator(String dbpath, HashMap<String,String[]> schema, String table, String[] joinTables, String[] columns, Expression whereExpression, String[] orderByColumns, boolean isDistinct, String[] groupByColumns, String sumExpression){
        this.duplicateEliminationOperator = new DuplicateEliminationOperator(dbpath, schema, table, joinTables, columns, whereExpression, orderByColumns, isDistinct);
        this.selectItems = columns;
        if(groupByColumns != null){
            this.isGroupBy = true;
            this.groupByColumns = groupByColumns;
        }
        if(sumExpression != null){
            this.isSum = true;
            this.sumExpression = sumExpression;
        }
    }

    // TODO: Implements methods getNextTuple, reset, dump and GROUP-BY.
    @Override
    Tuple getNextTuple(){
        System.out.println("We don't use getNextTuple in Sum Operator");
        return null;
    }

    @Override
    void reset(){
        this.duplicateEliminationOperator.reset();
    }

    @Override
    ArrayList<Tuple> dump() {
        String[] sumItems; // For a product
        String sumItem;    // For an integer or an item.
        ArrayList<Tuple> tuples = this.duplicateEliminationOperator.dump();
        if(this.isGroupBy){
            sumHashMap = new HashMap<String, Long>();
            tupleHashMap = new HashMap<String, Tuple>();
            // Determine Sum Expression
            // 1. Product
            if(this.sumExpression.contains("*")){
                System.out.println("Find * in Sum Expression");
                sumItems = sumExpression.split("\\*");
                for(int i = 0; i < sumItems.length; i++){
                    sumItems[i] = sumItems[i].trim();
                    System.out.println("SumItems: " + sumItems[i]);
                }
                for(Tuple t:tuples){
                    //  Key: "groupByColumns[0]+groupByColumns[1]+..."
                    //  Val: SUM
                    String key = getKey(t);
                    tupleHashMap.put(key, t);
                    sumHashMap.put(key, sumHashMap.getOrDefault(key, (long)0) + t.tuple.get(sumItems[0]).getValue() * t.tuple.get(sumItems[1]).getValue());
                }

            }
            // TODO
            //  2. Integer
            //  3. A column

            // Produce new tuples from hashmaps
            tuples.clear();
            for(String key:sumHashMap.keySet()){
                LinkedHashMap<String, LongValue> newLinkedHashMap = new LinkedHashMap<String, LongValue>();
//                for(String selectItem:this.selectItems){
                for(int i = 0; i < selectItems.length - SQLInterpreter.sumItemsCounter; i++){
                    newLinkedHashMap.put(selectItems[i], tupleHashMap.get(key).tuple.get(selectItems[i]));
                }
                newLinkedHashMap.put(sumExpression, new LongValue(sumHashMap.get(key)));
                tuples.add(new Tuple(newLinkedHashMap));
            }
        }
        return tuples;
    }

    public String getKey(Tuple tuple){
        String key = "";
        for(String column:this.groupByColumns){
            key += tuple.tuple.get(column).toString() + ",";
        }
        key = key.substring(0, key.length() - 1);
        System.out.println("GroupBy: Get new key: " + key);
        return key;
    }
}
