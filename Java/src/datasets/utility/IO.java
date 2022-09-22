package datasets.utility;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Contains methods to print result sets to System.out or a sql.csv-file.
 */
public class IO {
    public static void printResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println();
        }
    }

    public static void writeResultSetToCsv(String targetDirectory, String fileName, ResultSet resultSet) throws SQLException, IOException {
        File file = new File(targetDirectory + "/" + fileName + ".csv");
        file.createNewFile();

        try (FileWriter fileWriter = new FileWriter(file)) {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            StringBuffer sb = new StringBuffer();
            for (int i = 1; i < rsmd.getColumnCount(); i++) {
                sb.append(rsmd.getColumnName(i)).append(',');
            }
            sb.append(rsmd.getColumnName(rsmd.getColumnCount())).append('\n');
            fileWriter.write(sb.toString());
            sb.setLength(0);

            while (resultSet.next()) {
                for (int i = 1; i < rsmd.getColumnCount(); i++) {
                    sb.append(resultSet.getString(i)).append(',');
                }
                sb.append(resultSet.getString(rsmd.getColumnCount())).append('\n');
                fileWriter.write(sb.toString());
                sb.setLength(0);
            }
        }
    }
}
