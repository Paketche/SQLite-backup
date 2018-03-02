import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

public class Driver {

	public static void main(String[] args) {
		try {
//			System.out.println(args[0]);
//			System.out.println(args[1]);
//			if (!(args[0].endsWith(".db") && args[1].endsWith(".sql"))) {
//				throw new IndexOutOfBoundsException();
//			}

			// SQLClient c = new SQLClient("./Databases/LSH.db");
			// c.printResults(c.getMetaData().getColumns(null, null, "planets", "%"));

			DBbackup backup = new DBbackup(new SQLClient("./src/LSH.db"));

			File output = new File("./src/LSH.sql");
			if (!output.exists())
				output.createNewFile();

			BufferedWriter writer = new BufferedWriter(new FileWriter(output));

			backup.constructCreateStatements(writer);
			backup.constructInserts(writer);
			writer.close();

		} catch (IndexOutOfBoundsException e) {
			System.out.println("in the exception index");
			e.printStackTrace();
			System.out.println("arguments are missing or are not in order;\n"
					+ " make sure that the .db file is the first argument and the .sql file is the second");
		} catch (IOException e) {
			System.out.println("in the exception io");
			System.out.println(e.getMessage());
		} catch (SQLException e) {
			System.out.println("in the exception sql");
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}

}
