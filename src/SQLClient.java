
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class SQLClient {

	private static final String JDBC_DRIVER = "org.sqlite.JDBC";
	private static final String DATABASE_LOCATION = "jdbc:sqlite:";
	private Connection conn;
	private String dbName;

	public SQLClient(String path) throws IOException {
		File db = new File(path);

		if (!db.exists()) {
			throw new IOException();
		}

		dbName = db.getName();
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DATABASE_LOCATION + path);
			conn.setAutoCommit(false);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Returns the name of the database
	 * 
	 * @return the name of the database
	 */
	public String getDataBaseName() {
		return dbName;
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return conn.getMetaData();
	}

	/**
	 * Returns a list of all the table names;
	 * 
	 * @return a list of all the table names
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public ArrayList<String> getTableNames() throws SQLException {
		ArrayList<String> tables = new ArrayList<>();

		metaDataUser((meta) -> meta.getTables(null, null, "%", new String[] { "TABLE" }),
				(rs) -> tables.add(rs.getString("TABLE_NAME")));

		return tables;
	}

	/**
	 * Queries the database for the columns of a table and iterates over the results
	 * 
	 * @param tableName
	 *            name of the table whose columns are to be retrieved
	 * @param consumer
	 *            function for using the current iteration of the result set
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void forEachRowinTable(String tableName, CheckedConsumer<ResultSet> consumer) throws SQLException {
		metaDataUser(meta -> meta.getColumns(null, null, tableName, "%"), consumer);
	}

	/**
	 * Executes an sql query and iterates over its resutl set
	 * 
	 * @param sql
	 *            statement to be executed
	 * @param consumer
	 *            function to use the current iteration of the result set
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void executeQuery(String sql, CheckedConsumer<ResultSet> consumer) throws SQLException {
		Statement stmt = conn.createStatement();

		try (ResultSet rs = stmt.executeQuery(sql)) {
			resultSetIterator(rs, consumer);
		}
	}

	/**
	 * Queries the database meta data and passes each entry of the result set to the
	 * consumer
	 * 
	 * @param function
	 *            to query the meta data of the database
	 * @param consumer
	 *            function to use the current iteration in the result set
	 * @return true if the result set from querying the meta data was not empty;
	 * @throws SQLException
	 *             if a database access error occurs or this method is called on a
	 *             closed connection
	 */
	public boolean metaDataUser(CheckedFunction<DatabaseMetaData, ResultSet> function,
			CheckedConsumer<ResultSet> consumer) throws SQLException {

		DatabaseMetaData meta = conn.getMetaData();
		try (ResultSet rs = function.apply(meta)) {
			return resultSetIterator(rs, consumer);
		}
	}

	/**
	 * Iterates over a result set
	 * 
	 * @param resultSet
	 *            to be iterated over
	 * @param consumer
	 *            function to use the current iteration in the result set
	 * 
	 * @return true if the result set was not empty;
	 * @throws SQLException
	 *             if a database access error occurs or this method is called on a
	 *             closed connection
	 */
	public boolean resultSetIterator(ResultSet resultSet, CheckedConsumer<ResultSet> consumer) throws SQLException {
		if (resultSet.isAfterLast())
			return false;

		while (resultSet.next()) {
			consumer.accept(resultSet);
		}
		return true;
	}

	/**
	 * Prints the result set os a querry. the state ment that produced this result
	 * set must not be closed
	 * 
	 * @param results
	 *            result set of a querry
	 */
	public void printResults(ResultSet results) throws SQLException {
		ResultSetMetaData md = results.getMetaData();
		int colcount = md.getColumnCount();
		StringBuilder builder = new StringBuilder();

		for (int i = 1; i <= colcount; i++) {
			builder.append(pad("| " + md.getColumnLabel(i)));
		}
		builder.append("\r\n");

		int lenght = builder.length();
		for (int i = 0; i < lenght; i++) {
			builder.append("-");
		}
		builder.append("\r\n");

		while (results.next()) {
			for (int i = 1; i <= colcount; i++) {
				builder.append(pad("| " + results.getString(i)));
			}
			builder.append("\r\n");
		}

		System.out.println(builder.toString());
	}
	
	static private final int STR_SIZE = 25;

	// this method takes a String, converts it into an array of bytes;
	// copies those bytes into a bigger byte array (STR_SIZE worth), and
	// pads any remaining bytes with spaces. Finally, it converts the bigger
	// byte array back into a String, which it then returns.
	// e.g. if the String was "s_name", the new string returned is
	// "s_name                    " (the six characters followed by 18 spaces).
	private String pad(String in) {
		byte[] org_bytes = in.getBytes();
		byte[] new_bytes = new byte[STR_SIZE];
		int upb = in.length();

		if (upb > STR_SIZE)
			upb = STR_SIZE;

		for (int i = 0; i < upb; i++)
			new_bytes[i] = org_bytes[i];

		for (int i = upb; i < STR_SIZE; i++)
			new_bytes[i] = ' ';

		return new String(new_bytes);
	}
}
