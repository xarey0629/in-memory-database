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
    HashMap<String, String[]> schema;
    boolean isScan = false;

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

                parseSchema(this.dbPath);
                /**
                 * Get Select Items
                 * plainSelect.getSelectItems()
                 * Get From Items
                 * plainSelect.getFromItem().toString()
                 * Get Where Items
                 * plainSelect.getWhere() -> Expression
                 */
                String[] selectItems = ArrListToStringArr((ArrayList)plainSelect.getSelectItems());
                String table = plainSelect.getFromItem().toString();

                // Test Scan.
//                Operator operator = new ScanOperator(this.dbPath, this.schema, table);
//                ArrayList<Tuple> tuples = operator.dump();

                // Test Select
                Expression whereExpression = plainSelect.getWhere();

//                Operator operator = new SelectOperator(this.dbPath, this.schema, table, whereExpression);
//                ArrayList<Tuple> tuples = selectOperator.dump();

                // TODO: Test ProjectOperator
                Operator operator = new ProjectOperator(this.dbPath, this.schema, table, selectItems, whereExpression);
                ArrayList<Tuple> tuples = operator.dump();

                // TODO: Decide when to use ScanOperator, SelectOperator or ProjectOperator.
                // TODO: selectItems = null if Select *
                // TODO: whereExpression = null if no WHERE clause

                writeFile(outputFile, tuples);

            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
    }

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
     * Transfer Select Items to String[]
     * @param arrList
     * @return String[]
     */
    public String[] ArrListToStringArr(ArrayList arrList){
        if(arrList.size() == 0) return null;
        if(arrList.get(0) == "*") return null;

        String[] strArr = new String[arrList.size()];
        for(int i = 0; i < arrList.size(); i++){
            strArr[i] = arrList.get(i).toString();
        }
        return strArr;
    }

    public void writeFile(String outputFile, ArrayList<Tuple> tuples){
        File oFile = new File(outputFile);
        if (!oFile.getParentFile().exists()) {
            oFile.getParentFile().mkdirs();
        }
        System.out.println(oFile.getPath());
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
