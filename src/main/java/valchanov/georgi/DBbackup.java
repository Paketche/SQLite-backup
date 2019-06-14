package valchanov.georgi;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;

public class DBbackup {

    private SQLClient client;

    /**
     * Creates a new
     *
     * @param client
     */
    public DBbackup(SQLClient client) {
        this.client = client;
    }

    /**
     * * Constructs the foreign key statements for insertion in a CREATE TABLE
     * statement.
     *
     * @param tableName
     *            name of the table whose foreign keys are going to be retrieved
     * @return a string of the foreign key statements for the creation of a table
     *         (the string has a trailing ',\n'), if the table does not have foreign
     *         keys it returns an empty string(without trailing ',\n')
     * @throws SQLException
     *             if a database access error occurs
     */
    private String constFKStmt(String tableName) throws SQLException {
        StringBuilder builder = new StringBuilder();

        // gets the foreign keys of the table
        CheckedFunction<DatabaseMetaData, ResultSet> getFkeys = (meta) -> meta.getImportedKeys(null, null, tableName);

        // FOREIGN KEY (FKCOLUMN_NAME) REFERENCES PKTABLE_NAME(PKCOLUMN_NAME)
        CheckedConsumer<ResultSet> makeStmt = (rs) -> builder.append("\tFOREIGN KEY (" + rs.getString("FKCOLUMN_NAME")
                + ")\n\t\tREFERENCES " + rs.getString("PKTABLE_NAME") + "(" + rs.getString("PKCOLUMN_NAME") + "),\n");

        // for each key
        boolean hasF = client.metaDataUser(getFkeys, makeStmt);

        // if there were foreign keys
        return hasF ? builder.toString() : "";
    }

    /**
     *
     * Constructs the primary key statements for insertion in a CREATE TABLE
     * statement.
     *
     * @param tableName
     *            name of the table whose primary keys are going to be retrieved
     * @return a string of the primary key statement for the creation of a table
     *         (the string has a trailing ',\n'), if the table does not have primary
     *         keys it returns an empty string(without trailing ',\n')
     * @throws SQLException
     *             if a database access error occurs
     */
    private String constPKStmt(String tableName) throws SQLException {
        StringBuilder builder = new StringBuilder("\t" + "PRIMARY KEY" + " (");

        // gets the primary keys of the table
        CheckedFunction<DatabaseMetaData, ResultSet> getPkeys = (meta) -> meta.getPrimaryKeys(null, null, tableName);

        // PRIMARY KEY (COLUMN_NAME, ...)
        CheckedConsumer<ResultSet> makeStmt = (rs) -> builder.append(rs.getString("COLUMN_NAME") + ", ");

        // for each key
        boolean hasP = client.metaDataUser(getPkeys, makeStmt);

        // if there were primary keys
        if (hasP) {
            // remove trailing ", "
            int length = builder.length();
            builder.replace(length - 2, length, "),\n");
            return builder.toString();
        }
        return "";

    }

    /**
     * Constructs the CREATE statements of the connected database
     *
     * @param writer
     *            to which the statement are going to be written to
     * @throws SQLException
     *             if a database access error occurs
     * @throws IOException
     *             if an I/O error occurs
     */
    public void constructCreateStatements(Writer writer) throws SQLException, IOException {
        StringBuilder builder = new StringBuilder();
        ArrayList<String> tableNames = client.getTableNames();

        for (String tableName : tableNames) {
            builder.append("CREATE TABLE " + tableName + " (\n");

            // makes a statement for a column in a table
            CheckedConsumer<ResultSet> makeStmt = (rs) -> builder.append("\t" + rs.getString("COLUMN_NAME") + " "
                    + rs.getString("TYPE_NAME") + (rs.getString("IS_NULLABLE").equals("N") ? " NOT NULL" : "") + ",\n");

            // append each column name and domain
            client.forEachRowinTable(tableName, makeStmt);

            // get primary keys
            builder.append(constPKStmt(tableName));

            // get foreign keys
            builder.append(constFKStmt(tableName));

            // delete trailing comma and add closing bracket
            builder.deleteCharAt(builder.length() - 2);
            builder.append(");\n");
        }

        writer.write(builder.toString());
    }

    /**
     * Constructs the INSERT statements of the connected database
     *
     * @param writer
     *            to which the statement are going to be written to
     * @throws SQLException
     *             if a database access error occurs
     * @throws IOException
     *             if an I/O error occurs
     */
    public void constructInserts(BufferedWriter writer) throws SQLException, IOException {
        StringBuilder builder = new StringBuilder();
        ArrayList<String> tableNames = client.getTableNames();

        // maps column names to domains(keeps the inserts in ordered)
        LinkedHashMap<String, String> map = new LinkedHashMap<>();

        for (String tableName : tableNames) {

            // for each column map its name to it's domain
            CheckedConsumer<ResultSet> mapper = (rs) -> map.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"));
            client.forEachRowinTable(tableName, mapper);

            // to iterate over
            Set<String> columnNames = map.keySet();

            // make an insert statement for a row
            CheckedConsumer<ResultSet> makeStmt = (rs) -> {

                builder.append("INSERT INTO " + tableName + " VALUES (");

                for (String columnName : columnNames) {

                    String domain = map.get(columnName);

                    // get the value and make a string representation of it
                    Object value = rs.getObject(columnName);
                    String valueStr = "" + value;

                    // check if the value is not null
                    if (value != null) {
                        valueStr = value.toString();
                        // if the domain is not an int put surrounding quotes
                        if (valueStr.contains("\\N") || valueStr.equals(""))
                            valueStr = "null";
                        else if (domain.contains("CHAR")) {
                            valueStr = "\"" + valueStr + "\"";
                            // strange cases
                        }

                    } else {
                        valueStr = "null";
                    }
                    builder.append(valueStr + ", ");
                }

                // remove trailing ", "
                int l = builder.length();
                builder.replace(l - 2, l, ");\n");
            };

            // get all the tuples and add an insert statement to the builder
            client.executeQuery("SELECT * FROM " + tableName, makeStmt);

            // clear after finishing with table
            map.clear();
        }

        writer.write(builder.toString());
    }

}
