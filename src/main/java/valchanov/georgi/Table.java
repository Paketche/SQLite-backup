package valchanov.georgi;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static valchanov.georgi.Utils.commaSeparatedList;

public class Table implements Comparable<Table> {
    private final String name;
    private Map<String, Column> columns;

    private ArrayList<Column> primaryKeys;
    private ArrayList<Reference> references;

    private Table(String name, Map<String, Column> columns, ArrayList<Column> primaryKeys) {
        this.name = name;
        this.columns = columns;
        this.primaryKeys = primaryKeys;
        this.references = new ArrayList<>();
    }

    private Table(String name) {
        this(name, new TreeMap<>(), new ArrayList<>());
    }

    static List<Table> extractsTablesFrom(DatabaseMetaData metaData) throws SQLException {
        return Utils.collectResultSetToList(
                metaData.getTables(null, null, Utils.ANY_PATTERN, new String[]{"TABLE"}),
                rs -> new Table(rs.getString("TABLE_NAME"))
        );
    }


    public void addReference(String columnName, Reference reference) {
        columns.computeIfPresent(columnName, (cn, column) -> {
            column.setReference(reference);
            return column;
        });
    }

    public Column getColumn(String columnName) {
        return columns.get(columnName);
    }

    /**
     * @param column
     */
    public void addColumn(Column column) {
        this.columns.putIfAbsent(column.getName(), column);
    }

    public void overrideColumn(Column column) {
        this.columns.put(column.getName(), column);
    }

    public String toSQL(boolean soft, boolean formatted) {
        String formatting = formatted ? "\n\t" : "";

        String dropTable, ifDoesNotExist;
        if (soft) {
            dropTable = "";
            ifDoesNotExist = "IF NOT EXISTS";

        } else {
            dropTable = format("DROP TABLE IF EXIST %s;%s", name, formatting);
            ifDoesNotExist = "";
        }

        String tableContents = commaSeparatedList(
                asList(
                        commaSeparatedList(columns.values(), Column::toSQL, formatting),
                        format("PRIMARY KEY (%s)", commaSeparatedList(primaryKeys, Column::getName, formatting)),
                        commaSeparatedList(references, Reference::toString, formatting)
                ),
                formatting
        );


        return MessageFormat.format(
                "{1}CREATE TABLE {2} {3} ({0}{4}{0});{0}",
                formatting,/*0*/
                dropTable,/*1*/
                ifDoesNotExist,/*2*/
                name,/*3*/
                tableContents/*4*/
        );
    }


    public String getName() {
        return name;
    }

    @Override
    public int compareTo(Table otherTable) {

        //if the other table references this one, this should be ordered before it
        if (otherTable.columns.values().stream()
                .map(Column::getReference)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .anyMatch(r -> r.getReferencedTableName().equals(name))) {

            return 1;
        }

        //if thi table references the other table, this should be ordered after it
        if (this.columns.values().stream()
                .map(Column::getReference)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .anyMatch(r -> r.getReferencedTableName().equals(otherTable.name))) {

            return -1;
        }

        //if they do not reference each other order does not matter.
        return 0;
    }
}
