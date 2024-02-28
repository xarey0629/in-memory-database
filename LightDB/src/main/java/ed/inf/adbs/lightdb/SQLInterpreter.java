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


public class SQLInterpreter {
    String dbPath;
    String inputFile;
    String outputFile;
    HashMap<String, String[]> schema;

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
                System.out.println("Select body is " + select.getSelectBody());

                PlainSelect plainSelect = (PlainSelect) select.getPlainSelect();
                /**
                 * Get Select Items
                 * plainSelect.getSelectItem(indices)
                 * Get From Items
                 * plainSelect.getFromItem().toString()
                 * Get Where Items
                 * plainSelect.getWhere() -> Expression
                 */

                String table = plainSelect.getFromItem().toString();
//                System.out.println("The table is " + table);

                parseSchema(this.dbPath);

                // Test Scan.
//                Operator operator = new ScanOperator(this.dbPath, this.schema, table);
//                ArrayList<Tuple> tuples = operator.dump();

                // TODO: Test Select
                Expression whereExpression = plainSelect.getWhere();
                Operator selectOperator = new SelectOperator(this.dbPath, this.schema, table, whereExpression);
                ArrayList<Tuple> tuples = selectOperator.dump();
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
