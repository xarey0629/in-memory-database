package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class SQLInterpreter {
    String dbPath;
    String inputFile;
    String outputFile;
    /**
     * Schema use hash map DS to store TableName/An array of Attributes paris.
     * For example: schema.get("Sailors") = {A, B, C}
     */
    HashMap<String, String[]> schema = null;
    public static boolean hasAlias = false;

    /**
     * Used to find real table names by aliases.
     * For example: aliasToTable.get("S") = "Sailors"
     */
    public static HashMap<String, String> aliasToTable = new HashMap<String, String>();
    /**
     * Remove Sum Expression from selectItems as sumExpression.
     */
    public  static String sumExpression = null;

    /**
     * Count how many NEW select items added in SUM Expression.
     */
    public static Integer sumItemsCounter = 0;


    /**
     * Basic constructor.
     * @param dbpath the path to db file.
     * @param inputfile the path to input file.
     * @param outputfile the path to output file.
     */
    SQLInterpreter(String dbpath, String inputfile, String outputfile){
        this.dbPath = dbpath;
        this.inputFile = inputfile;
        this.outputFile = outputfile;
    }

    public void interpret(){
        // Modified from example function: parsingExample()
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
//            Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Sailors");
            if (statement != null) {
                System.out.println("Read statement: " + statement);
                Select select = (Select) statement;
                PlainSelect plainSelect = (PlainSelect) select.getPlainSelect();
                /**
                 * Get Select Items
                 * plainSelect.getSelectItems()
                 * Get From Items
                 * plainSelect.getFromItem().toString()
                 * Get Where Items
                 * plainSelect.getWhere() -> Expression
                 */
                // Read and Load Schema
                parseSchema(this.dbPath);
                // Get Selected Attributes
                String[] selectItems = checkStarAndSumThenGetSelectItems((ArrayList)plainSelect.getSelectItems());
                // Get all Table Names.
                String table = plainSelect.getFromItem().toString();
                String[] leftTableNames = ArrListToStringArr((ArrayList)plainSelect.getJoins());
                // Get WHERE expression.
                Expression whereExpression = plainSelect.getWhere();
                // Get ORDER BY elements
                String[] orderByColumns = ArrListToStringArr((ArrayList)plainSelect.getOrderByElements());
                // Get DISTINCT elements
                boolean isDistinct = hasDistinct(plainSelect.getDistinct());
                // Get GROUP BY elements
                String[] groupByColumns = null;
                if(plainSelect.getGroupBy() != null) groupByColumns = ArrListToStringArr((ArrayList)plainSelect.getGroupBy().getGroupByExpressionList());


                // Test Select & Aliases
                this.hasAlias = isHasAlias(table, leftTableNames);
                if(hasAlias){
                    //  1. Update schema with aliases.
                    //  2. Update names of tables.
                    updateSchemaWithAliases(table, leftTableNames);
                    table = getTableAlias(table);
                    if(leftTableNames != null && leftTableNames.length >= 1){
                        leftTableNames = getJoinTableAliases(leftTableNames);
                    }

                }
//                Operator operator = new ScanOperator(this.dbPath, this.schema, table);
//                Operator operator = new SelectOperator(this.dbPath, this.schema, table, whereExpression);

                // Test ProjectOperator
//                Operator operator = new ProjectOperator(this.dbPath, this.schema, table, selectItems);
//                Operator operator = new ProjectOperator(this.dbPath, this.schema, table, selectItems, whereExpression);

                // TODO: Decide when to use ScanOperator, SelectOperator or ProjectOperator.
                // TODO: selectItems = null if Select *
                // TODO: whereExpression = null if no WHERE clause

                // Test JoinOperator
//                Operator operator = new JoinOperator(this.dbPath, this.schema, table, leftTableNames, whereExpression);

                // Test Sort Operator
//                Operator operator = new SortOperator(this.dbPath, this.schema, table, leftTableNames, selectItems, whereExpression, orderByColumns);

                // Test DuplicateEliminationOperator
//                Operator operator = new DuplicateEliminationOperator(this.dbPath, this.schema, table, leftTableNames, selectItems, whereExpression, orderByColumns, isDistinct);

                // Test SumOperator
                Operator operator = new SumOperator(this.dbPath, this.schema, table, leftTableNames, selectItems, whereExpression, orderByColumns, isDistinct, groupByColumns, sumExpression);

                // Write Output File
                ArrayList<Tuple> tuples = operator.dump();
                writeFile(outputFile, tuples);
            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
    }

    /**
     * Check is there * or SUM in SELECT clause.
     * Remove SUM from selectItems to this.sumExpression.
     * Add select items in SUM expression back to selectItems.
     * @param select_Items
     * @return
     */
    public String[] checkStarAndSumThenGetSelectItems(ArrayList select_Items) {
        ArrayList selectItemsList = select_Items;
        if(selectItemsList.get(0).toString().equals("*")) return  null;
        // Determine SUM
        else if(selectItemsList.get(selectItemsList.size() - 1).toString().contains("SUM")){
            System.out.println("Find SUM instruction: " + selectItemsList.get(selectItemsList.size() - 1).toString());
            this.sumExpression = selectItemsList.get(selectItemsList.size() - 1).toString();
            selectItemsList.remove(selectItemsList.size() - 1);

            // Add select items in SUM expression back to selectItems.
            String sumExpress = this.sumExpression.substring(4, this.sumExpression.length() - 1);
            this.sumExpression = sumExpress;
            System.out.println("Sum Expression without SUM(): " + sumExpress);
            String[] sumSelectItems = sumExpress.split("\\*");
            for(int i = 0; i < sumSelectItems.length; i++){
                sumSelectItems[i] = sumSelectItems[i].trim();
            }
            for(String str:sumSelectItems){
                System.out.println(str);
                if(!(str.equals("*") || isNumeric(str))){
                    System.out.println("Find sum select item: " + str);
                    if(selectItemsList.contains(str)){
                        System.out.println("Sum select item already exists in selectItems.");
                    }else{
                        System.out.printf("Add sum select item: %s into selectItems.\n", str);
                        selectItemsList.add(str);
                        sumItemsCounter++;
                    }
                }
            }
        }
        if(selectItemsList.size() == 0) return null;
        return ArrListToStringArr(selectItemsList);
    }

    /**
     * Check a string is number or not.
     * @param str
     * @return
     */
    public static boolean isNumeric(String str) {
        try {
            double d = Double.parseDouble(str);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    public boolean hasDistinct(Distinct distinct) {
        return distinct != null;
    }

    /**
     * Load Schema.txt into this.schema Data Structure (Hash Map).
     * @param dbpath
     */
    public void parseSchema(String dbpath) {
        this.schema = new HashMap<String, String[]>();
        try{
            FileReader fileReader = new FileReader(dbpath + "/schema.txt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = null;
            while((line = bufferedReader.readLine()) != null) {
                String[] names = line.split(" ");
                String table_id = names[0];
                String[] attributes = Arrays.copyOfRange(names, 1, names.length);
                this.schema.put(table_id, attributes);
            }
        }
        catch (FileNotFoundException fnfE){
            fnfE.printStackTrace();
            System.exit(1);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check is there any alias.
     * @param table
     * @param joinTables
     * @return
     */
    public boolean isHasAlias(String table, String[] joinTables){
        if(table.contains(" ")) return true;
        if(joinTables == null) return false;
        for(int i = 0; i < joinTables.length; i++){
            if(joinTables[i].contains(" ")) return true;
        }
        return false;
    }

    /**
     * Update this.schema from Full names to Aliases.
     * Take care of self-joins
     * (Full name / Attributes) paris still remain in the hash map, so a full name of table could still be used.
     * @param firstTable
     * @param joinTables
     */
    public void updateSchemaWithAliases(String firstTable, String[] joinTables){
        if(firstTable.contains(" ")){
            String[] firstTableAlias = firstTable.split(" ");
            String[] oldValues = this.schema.get(firstTableAlias[0]);
            this.schema.put(firstTableAlias[1], oldValues);
            this.aliasToTable.put(firstTableAlias[1], firstTableAlias[0]);
        }
        if(joinTables == null) return;
        for(int i = 0; i < joinTables.length; i++){
            if(joinTables[i].contains(" ")){
                String[] TableAlias = joinTables[i].split(" ");
                String[] oldValues = this.schema.get(TableAlias[0]);
                this.schema.put(TableAlias[1], oldValues);
                this.aliasToTable.put(TableAlias[1], TableAlias[0]);
            }
        }
    }

    /**
     * Transfer ArrayList to String[]
     * @param arrList
     * @return String[]
     */
    public String[] ArrListToStringArr(ArrayList arrList){
        if(arrList == null) return null;
        if(arrList.get(0) == "*") return null; // This take care of "*" and "SUM()" in SELECT clause, which tells the difference between Scan and Project operators.
        String[] strArr = new String[arrList.size()];
        for(int i = 0; i < arrList.size(); i++){
            strArr[i] = arrList.get(i).toString();
        }
        return strArr;
    }

    public String getTableAlias(String table){
        if(table.contains(" ")){
            return table.split(" ")[1];
        }else return table;
    }

    public String[] getJoinTableAliases(String[] joinTable){
        String[] aliasesArr = joinTable;
        for(int i = 0; i < joinTable.length; i++){
            if(joinTable[i].contains(" ")){
                aliasesArr[i] = joinTable[i].split(" ")[1];
            }
        }
        return aliasesArr;
    }

    public void writeFile(String outputFile, ArrayList<Tuple> tuples){
        File oFile = new File(outputFile);
        if (!oFile.getParentFile().exists()) {
            oFile.getParentFile().mkdirs();
        }
        System.out.print("Write file to: " + oFile.getPath() + '\n');
        try{
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(oFile));
            for(Tuple tuple:tuples){
                bufferedWriter.write(tuple.printTuple());
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
