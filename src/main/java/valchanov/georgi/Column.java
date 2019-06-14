package valchanov.georgi;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;
import static valchanov.georgi.Utils.ANY_PATTERN;
import static valchanov.georgi.Utils.collectResultToMapOfLists;

public class Column {
    private String name;
    private FieldTypes type;
    private Reference reference;
    private Set<Constraints> constraints;

    public Column(String name, FieldTypes type) {
        this(name, type, null, new HashSet<>());
    }

    public Column(String name, FieldTypes type, Reference reference) {
        this(name, type, reference, new LinkedHashSet<>());
    }

    public Column(String name, FieldTypes type, Set<Constraints> constraints) {
        this(name, type, null, constraints);
    }

    public Column(String name, FieldTypes type, Reference reference, Set<Constraints> constraints) {
        this.name = name;
        this.type = type;
        this.reference = reference;
        this.constraints = constraints;
    }

    /**
     * Extracts the columns in database given a table name pattern.
     *
     * @param tableNamePatter to match against.
     * @param metaData        from which to query the columns
     * @return a map with table names as keys to their respective columns
     * @throws SQLException if a database access error occurs
     */
    static Map<String, List<Column>> extractColumnsFrom(String tableNamePatter, DatabaseMetaData metaData) throws SQLException {
        return collectResultToMapOfLists(
                metaData.getColumns(null, null, tableNamePatter, ANY_PATTERN),
                rs -> rs.getString("TABLE_NAME"),
                rs -> {
                    FieldTypes type = FieldTypes.valueOf(rs.getString("TYPE_NAME"));

                    Set<Constraints> constraints = new HashSet<>();
                    if (!rs.getString("IS_NULLABLE").equals("YES")) constraints.add(Constraints.NOT_NULL);
                    if (rs.getString("IS_AUTOINCREMENT").equals("YES")) {
                        constraints.add(Constraints.AUTOINCREMENT);
                        constraints.add(Constraints.PRIMARY_KEY);
                    }


                    return new Column(rs.getString("COLUMN_NAME"), type, constraints);
                }
        );
    }

    public void addConstraint(Constraints constraint) {
        this.constraints.add(constraint);
    }

    public void makePrimaryKey() {
        this.constraints.add(Constraints.PRIMARY_KEY);
    }

    public boolean isPrimaryKey() {
        return this.constraints.contains(Constraints.PRIMARY_KEY);
    }

    public String toSQL() {
        return String.format(
                "%s %s %s",
                name,
                type,
                constraints
                        .stream()
                        .map(Constraints::toString)
                        .collect(Collectors.joining(" "))
        );
    }

    public Optional<String> getReferenceSQL() {
        if (nonNull(reference)) {
            return Optional.of(reference.toSQL(name));
        }
        return Optional.empty();
    }

    public Optional<Reference> getReference() {
        return Optional.ofNullable(reference);
    }

    public void setReference(Reference reference) {
        this.reference = reference;
    }

    public String getName() {
        return name;
    }
}
