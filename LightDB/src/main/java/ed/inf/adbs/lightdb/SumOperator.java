package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

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
        ArrayList<Tuple> tuples = this.duplicateEliminationOperator.dump();
        if(!(isGroupBy || isSum)) return tuples;

        LinkedHashSet<String> keySet = new LinkedHashSet<>();
        sumHashMap = new HashMap<String, Long>();
        tupleHashMap = new HashMap<String, Tuple>();
        String[] sumItems;          // For a product
        String sumColItem;          // For a column
        long sumLongItem;            // For a long

        if(this.isGroupBy){
            for(Tuple t:tuples){
                // Key: "groupByColumns[0]+groupByColumns[1]+..."
                // Val: Tuple
                String key = getKey(t);
                keySet.add(key);
                tupleHashMap.put(key, t);
            }
        }else{
            for(Tuple t:tuples){
                // Key: "groupByColumns[0]+groupByColumns[1]+..."
                // Val: Tuple
                String key = "UNIVERSAL_KEY";
            }
            keySet.add("UNIVERSAL_KEY");
        }
        // Determine Sum Expression
        if(this.isSum){
            //  1. Product
            if(this.sumExpression.contains("*")){
                System.out.println("Find * in Sum Expression");
                sumItems = sumExpression.split("\\*");
                for(int i = 0; i < sumItems.length; i++){
                    sumItems[i] = sumItems[i].trim();
                    System.out.println("SumItems: " + sumItems[i]);
                }
                for(Tuple t:tuples){
                    // Key: "groupByColumns[0]+groupByColumns[1]+..."
                    String key;
                    if(isGroupBy) key = getKey(t);
                    else key = "UNIVERSAL_KEY";
                    // Val: Product SUM
                    // Calculate the product.
                    long product = 1;
                    for(String sumItem:sumItems){
                        if(SQLInterpreter.isNumeric(sumItem)){
                            product *= Long.parseLong(sumItem);
                        }else product *= t.tuple.get(sumItem).getValue();
                    }
                    sumHashMap.put(key, sumHashMap.getOrDefault(key, (long)0) + product);
                }
            }
            //  2. An Integer
            else if(SQLInterpreter.isNumeric(this.sumExpression)){
                System.out.println("Find an Integer in Sum Expression");
                sumLongItem = Long.parseLong(this.sumExpression);
                for(Tuple t:tuples){
                    String key;
                    if(isGroupBy) key = getKey(t);
                    else key = "UNIVERSAL_KEY";
//                    tupleHashMap.put(key, t);
                    sumHashMap.put(key, sumHashMap.getOrDefault(key, (long)0) + sumLongItem);
                }
            }
            //  3. A column
            else{
                sumColItem = sumExpression;
                System.out.println("Find a Column in Sum Expression: " + sumColItem);
                for(Tuple t:tuples){
                    // Key: "groupByColumns[0]+groupByColumns[1]+..."
                    String key;
                    if(isGroupBy) key = getKey(t);
                    else key = "UNIVERSAL_KEY";
                    // Val: SUM
                    sumHashMap.put(key, sumHashMap.getOrDefault(key, (long)0) + t.tuple.get(sumColItem).getValue());
                }
            }
        }
        // Produce new tuples from hashmaps
        ArrayList<Tuple> newTuples = tuples;
        if(this.isGroupBy){
            tuples.clear();
            for(String key:keySet){
                LinkedHashMap<String, LongValue> newLinkedHashMap = new LinkedHashMap<String, LongValue>();
                // Add Select Items back.
                if(selectItems != null){
                    for(int i = 0; i < selectItems.length - SQLInterpreter.sumItemsCounter; i++){
                        newLinkedHashMap.put(selectItems[i], tupleHashMap.get(key).tuple.get(selectItems[i]));
                    }
                }
                // Add SUM item.
                if(isSum){
                    newLinkedHashMap.put(sumExpression, new LongValue(sumHashMap.get(key)));
                }
                tuples.add(new Tuple(newLinkedHashMap));
            }
            return tuples;
        }else{
            // SelectItems are needed.
            if(selectItems.length - SQLInterpreter.sumItemsCounter > 0){
                for(Tuple t:tuples){
                    LinkedHashMap<String, LongValue> newLinkedHashMap = new LinkedHashMap<String, LongValue>();
                    // Add Select Items back.
                    for(int i = 0; i < selectItems.length - SQLInterpreter.sumItemsCounter; i++){
                        newLinkedHashMap.put(selectItems[i], t.tuple.get(selectItems[i]));
                    }
                    // Add SUM item.
                    if(isSum){
                        newLinkedHashMap.put(sumExpression, new LongValue(sumHashMap.get("UNIVERSAL_KEY")));
                    }
                    newTuples.add(new Tuple(newLinkedHashMap));
                }
            }else{ // No SelectItems is needed.
                newTuples.clear();
                LinkedHashMap<String, LongValue> newLinkedHashMap = new LinkedHashMap<String, LongValue>();
                newLinkedHashMap.put(sumExpression, new LongValue(sumHashMap.get("UNIVERSAL_KEY")));
                newTuples.add(new Tuple(newLinkedHashMap));
            }
            return newTuples;
        }
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
