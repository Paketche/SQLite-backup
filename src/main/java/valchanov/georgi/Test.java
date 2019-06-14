package valchanov.georgi;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;


public class Test {
    public static void main(String[] args) throws SQLException, IOException {

        Database database = Database.extractFrom(Paths.get("C:\\Users\\valka\\University\\test.db"));
        database.backUpStructure(new BufferedWriter(new FileWriter(new File("C:\\Users\\valka\\University\\test2.db"))), true, true);


    }
}
