import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

public class Driver {

	public static void main(String[] args) {
		try {
			if (!(args[0].endsWith(".db") && args[1].endsWith(".sql"))) {
				throw new IndexOutOfBoundsException();
			}

			DBbackup backup = new DBbackup(new SQLClient(args[0]));

			File output = new File(args[1]);
			if (!output.exists())
				output.createNewFile();

			BufferedWriter writer = new BufferedWriter(new FileWriter(output));

			backup.constructCreateStatements(writer);
			backup.constructInserts(writer);
			writer.close();

		} catch (IndexOutOfBoundsException e) {
			System.out.println("arguments are missing or are not in order;\n"
					+ " make sure that the .db file is the first argument and the .sql file is the second");
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

}
