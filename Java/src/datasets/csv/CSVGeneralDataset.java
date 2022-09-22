package datasets.csv;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ResourceBundle;

/**
 * Creates a CSV-file with all information included in the general dataset.
 */
public class CSVGeneralDataset {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        ResultSet rs = statement.executeQuery("SELECT * FROM general_dataset WHERE 1 = 0");
        String header = Misc.getCSVHeader(rs);
        rs.close();

        String outDir = ResourceBundle.getBundle("resources").getString("output-directory");

        statement.execute("SELECT " + header + " " +
                "UNION ALL " +
                "SELECT * FROM general_dataset t1 WHERE NOT EXISTS (SELECT * FROM tasks_exclude t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX) " +
                "INTO OUTFILE '" + outDir + "/general-dataset.csv' " +
                "FIELDS TERMINATED BY ',' " +
                "LINES TERMINATED BY '\n'");

        statement.close();
        connection.close();
    }
}
