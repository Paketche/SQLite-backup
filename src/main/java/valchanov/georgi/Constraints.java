package valchanov.georgi;

public enum Constraints {
    NOT_NULL,
    PRIMARY_KEY,
    AUTOINCREMENT;

    @Override
    public String toString() {
        return super.toString().replace("_", " ");
    }


}
