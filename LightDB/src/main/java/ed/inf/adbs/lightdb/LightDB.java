package ed.inf.adbs.lightdb;

import java.io.FileReader;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) {
		try{
			if (args.length != 3) {
				System.err.println("Wrong input detected.");
				return;
			}
			// Create an interpreter to execute the query.
			// input: dbPath, inputFile, outputFile
			SQLInterpreter sqlInterpreter = new SQLInterpreter(args[0], args[1], args[2]);
			sqlInterpreter.interpret();
		}catch (Exception e){
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */

	public static void parsingExample(String filename) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
//            Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Boats");
			if (statement != null) {
//				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
//				System.out.println("Select body is " + select.getSelectBody());
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}
