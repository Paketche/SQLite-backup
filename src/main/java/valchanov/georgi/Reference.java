package valchanov.georgi;

public class Reference {
    private final String referencedColumnName;
    private final String referencedTableName;

    public Reference(String referencedColumnName, String referencedTableName) {
        this.referencedColumnName = referencedColumnName;
        this.referencedTableName = referencedTableName;
    }

    public String getReferencedColumnName() {
        return referencedColumnName;
    }

    public String getReferencedTableName() {
        return referencedTableName;
    }

    public String toSQL(String localColumnName) {
        return String.format("FOREIGN KEY (%s) REFERENCES %s(%s)", localColumnName, referencedTableName, referencedColumnName);
    }
}
