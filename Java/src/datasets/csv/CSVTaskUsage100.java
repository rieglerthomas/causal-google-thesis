package datasets.csv;

import datasets.utility.ConnectionWrapper;

import java.sql.*;
import java.util.Arrays;
import java.util.ResourceBundle;

/**
 * Creates a CSV-file with all information included in the task usage dataset.
 */
public class CSVTaskUsage100 {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM task_usage WHERE 1 = 0");
        ResultSetMetaData rsmd = resultSet.getMetaData();
        String[] columnNames = new String[rsmd.getColumnCount()];
        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = rsmd.getColumnLabel(i + 1);
        }
        String relevantColumns = getRelevantColumns(columnNames,"START_TIME","END_TIME","SAMPLE_PORTION","SAMPLED_CPU_USAGE");

        String outDir = ResourceBundle.getBundle("resources").getString("output-directory");

        String outfile = outDir + "/task_usage_100.csv";
        statement.execute("SELECT " + getHeader(relevantColumns) + " UNION ALL " +
                "SELECT " + relevantColumns + " " +
                "FROM task_usage_filtered t1 " +
                "WHERE NOT EXISTS (SELECT * FROM tasks_exclude t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX AND t2.REASON = 'No termination before trace end') " +
                "AND EXISTS (SELECT * FROM tasks_completeness_100 t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX) " +
                "INTO OUTFILE '" + outfile + "' " +
                "FIELDS TERMINATED BY ',' " +
                "LINES TERMINATED BY '\n'");

        statement.close();
        connection.close();
    }

    private static String getRelevantColumns(String[] columns, String... excludedColumns) {
        if (excludedColumns.length == 0) return "*";
        String ret = "";
        for (int i = 0; i < columns.length; i++) {
            int finalI = i;
            if (Arrays.stream(excludedColumns).noneMatch(s -> s.equals(columns[finalI]))) {
                ret += columns[finalI] + ",";
            }
        }
        return ret.substring(0, ret.length()-1);
    }

    private static String getHeader(String relevantColumns) {
        String[] strs = relevantColumns.split(",", -1);
        String ret = "";
        for (int i = 0; i < strs.length; i++) {
            ret += "'" +  strs[i] + "',";
        }
        return ret.substring(0, ret.length() - 1);
    }
}
