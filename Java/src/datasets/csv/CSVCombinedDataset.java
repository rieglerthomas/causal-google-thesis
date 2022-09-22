package datasets.csv;

import datasets.utility.ConnectionWrapper;

import java.sql.*;
import java.util.ResourceBundle;

/**
 * Creates a CSV-file with all information included in the combined dataset.
 */
public class CSVCombinedDataset {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM combined_dataset WHERE 1 = 0");
        String outDir = ResourceBundle.getBundle("resources").getString("output-directory");

        String outfile = outDir + "/combined-dataset.csv";
        statement.execute("SELECT " + getCSVHeader(resultSet) + " UNION ALL " +
                "SELECT * " +
                "FROM combined_dataset t1 " +
                "WHERE NOT EXISTS (SELECT * FROM tasks_exclude t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX) " +
                "INTO OUTFILE '" + outfile + "' " +
                "FIELDS TERMINATED BY ',' " +
                "LINES TERMINATED BY '\n'");
        
        resultSet.close();
        statement.close();
        connection.close();
    }
    
    private static String getCSVHeader(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        String ret = "";
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            ret += "'" +  rsmd.getColumnLabel(i) + "',";
        }
        return ret.substring(0, ret.length() - 1);
    }
}
