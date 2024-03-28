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
     * @param table right single child table in left-deep tree(on the right side).
     * @param joinTables rest of tables on the left side.
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

    // It's blocking, we don't get single tuple.
    @Override
    Tuple getNextTuple(){
//        System.out.println("We don't use getNextTuple in Sum Operator, it should be blocking");
        return null;
    }

    @Override
    void reset(){
        this.duplicateEliminationOperator.reset();
    }

    /**
     * Get all the tuples.
     * Logic is explained in comments inside.
     * @return grouped tuples or aggregation.
     */
    @Override
    ArrayList<Tuple> dump() {
        ArrayList<Tuple> tuples = this.duplicateEliminationOperator.dump();
        if(!(isGroupBy || isSum)) return tuples;

        LinkedHashSet<String> keySet = new LinkedHashSet<>();
        sumHashMap = new HashMap<String, Long>();
        tupleHashMap = new HashMap<String, Tuple>();
        String[] sumItems;          // For a product
        String sumColItem;          // For a column
        long sumLongItem;           // For a long

        // Determine GROUP BY Expression
        if(this.isGroupBy){
            for(Tuple t:tuples){
                // Key: "groupByColumns[0]+groupByColumns[1]+..."
                // Val: Tuple
                String key = getKey(t);
                keySet.add(key);
                tupleHashMap.put(key, t);
            }
        }else{
            keySet.add("UNIVERSAL_KEY");
        }

        // Determine SUM Expression
        if(this.isSum){
            //  1. Products
            if(this.sumExpression.contains("*")){
//                System.out.println("Find * in Sum Expression");
                sumItems = sumExpression.split("\\*");
                for(int i = 0; i < sumItems.length; i++){
                    sumItems[i] = sumItems[i].trim();
//                    System.out.println("SumItems: " + sumItems[i]);
                }
                for(Tuple t:tuples){
                    String key = getKey(t);

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
//                System.out.println("Find an Integer in Sum Expression");
                sumLongItem = Long.parseLong(this.sumExpression);
                for(Tuple t:tuples){
                    String key = getKey(t);
                    sumHashMap.put(key, sumHashMap.getOrDefault(key, (long)0) + sumLongItem);
                }
            }
            //  3. A column
            else{
                sumColItem = sumExpression;
//                System.out.println("Find a Column in Sum Expression: " + sumColItem);
                for(Tuple t:tuples){
                    String key = getKey(t);
                    sumHashMap.put(key, sumHashMap.getOrDefault(key, (long)0) + t.tuple.get(sumColItem).getValue());
                }
            }
        }

        // Produce new tuples by selecting proper attributes and adding SUM aggregation.
        ArrayList<Tuple> newTuples = new ArrayList<>();
        if(this.isGroupBy){
            for(String key:keySet){
                LinkedHashMap<String, LongValue> newLinkedHashMap = new LinkedHashMap<String, LongValue>();

                // Add Select Items back in proper order.
                if(selectItems != null){
                    for(int i = 0; i < selectItems.length - SQLInterpreter.sumItemsCounter; i++){
//                        System.out.println("Put SELECT: " + selectItems[i] + " back to tuple.");
                        newLinkedHashMap.put(selectItems[i], tupleHashMap.get(key).tuple.get(selectItems[i]));
                    }
                }
                // Add SUM item.
                if(isSum){
                    newLinkedHashMap.put("SUM(" + sumExpression + ")", new LongValue(sumHashMap.get(key)));
                }
                newTuples.add(new Tuple(newLinkedHashMap));
            }
        }else{
            // SelectItems are needed.
            if(selectItems.length - SQLInterpreter.sumItemsCounter > 0){
                for(Tuple t:tuples){
                    LinkedHashMap<String, LongValue> newLinkedHashMap = new LinkedHashMap<String, LongValue>();
                    // Add Select Items back.
                    for(int i = 0; i < selectItems.length - SQLInterpreter.sumItemsCounter; i++){
//                        System.out.println("Put SELECT: " + selectItems[i] + " back to tuple.");
                        newLinkedHashMap.put(selectItems[i], t.tuple.get(selectItems[i]));
                    }
                    // Add SUM item.
                    newLinkedHashMap.put("SUM(" + sumExpression + ")", new LongValue(sumHashMap.get("UNIVERSAL_KEY")));

                    newTuples.add(new Tuple(newLinkedHashMap));
                }
            }else{ // No SelectItems is needed.
                LinkedHashMap<String, LongValue> newLinkedHashMap = new LinkedHashMap<String, LongValue>();
                newLinkedHashMap.put(sumExpression, new LongValue(sumHashMap.get("UNIVERSAL_KEY")));
                newTuples.add(new Tuple(newLinkedHashMap));
            }
        }
        return newTuples;
    }

    /**
     * Get values from tuple and combine them to a group-by key.
     * If no group by, return a "UNIVERSAL_KEY";
     * @param tuple
     * @return group-by key string
     */
    public String getKey(Tuple tuple){
        if(!isGroupBy) return "UNIVERSAL_KEY";
        String key = "";
        for(String column:this.groupByColumns){
            key += tuple.tuple.get(column).toString() + ",";
        }
        key = key.substring(0, key.length() - 1);
//        System.out.println("GroupBy: Get new key: " + key);
        return key;
    }
}
