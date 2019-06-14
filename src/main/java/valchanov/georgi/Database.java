package valchanov.georgi;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.sql.DriverManager.getConnection;
import static java.util.stream.Collectors.toList;
import static valchanov.georgi.Utils.ANY_PATTERN;
import static valchanov.georgi.Utils.forEachResult;

public class Database implements Closeable {
    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private final Connection connection;
    private List<Table> tables;


    public Database(Path databaseLocation) throws SQLException, IOException {
        if (Files.exists(databaseLocation)) {
            Files.createFile(databaseLocation);
        }
        try {
            Class.forName(JDBC_DRIVER);

            this.connection = getConnection(format("jdbc:sqlite:%s", databaseLocation.toAbsolutePath().toString()));
            this.tables = new ArrayList<>();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Database(Connection connection, List<Table> tables) {
        this.connection = connection;
        this.tables = tables;
    }

    public static Database extractFrom(Path databaseLocation) throws SQLException {
        try {
            Class.forName(JDBC_DRIVER);

            Connection connection = getConnection(format("jdbc:sqlite:%s", databaseLocation.toAbsolutePath().toString()));
            DatabaseMetaData metaData = connection.getMetaData();

            //get tables
            List<Table> tables = Table.extractsTablesFrom(metaData);
            //get columns
            Map<String, List<Column>> columns = Column.extractColumnsFrom(ANY_PATTERN, metaData);

            for (Table table : tables) {
                //add columns to table
                columns.get(table.getName()).forEach(table::addColumn);

                //get primary keys
                forEachResult(
                        metaData.getPrimaryKeys(null, null, table.getName()),
                        rs -> table.getColumn(rs.getString("COLUMN_NAME")).makePrimaryKey()
                );

                //get foreign keys
                forEachResult(
                        metaData.getImportedKeys(null, null, table.getName()),
                        rs -> table.addReference(
                                rs.getString("FKCOLUMN_NAME"),
                                new Reference(
                                        rs.getString("PKCOLUMN_NAME"),
                                        rs.getString("PKTABLE_NAME")
                                )
                        )
                );
            }


            return new Database(connection, tables);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Table> getTables() {
        return tables;
    }

    public void backUpStructure(Writer writer, boolean formatted, boolean soft) throws IOException {

        //sort the tables by referencing and get their sql then print it
        for (String tableSQL :
                tables.stream()
                        .sorted()
                        .map(t -> t.toSQL(soft, formatted))
                        .collect(toList())) {
            writer.write(tableSQL);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw new IOException("SQL Exception occurred while closing the connection ", e);
        }
    }
}
