package datasets.utility;

import java.sql.*;
import java.util.ResourceBundle;

/**
 * Contains small, simple methods which are useful in certain situations.
 */
public class Misc {

    /**
     * Returns a comma-separated list of the column names from the given result set.
     * @param resultSet to be used
     * @return comma-separated list of the column names from the given result set
     * @throws SQLException if a database error occurs
     */
    public static String getHeader(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        String ret = "";
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            ret += resultSetMetaData.getColumnLabel(i) + ",";
        }
        return ret.substring(0, ret.length() - 1);
    }

    /**
     * Creates a header for a CSV-file since there is now (that I know of) which allows us to add the column names
     * of a result set to an exported CSV-file.
     *
     * @param resultSet result set to get the CSV-header for
     * @return a header which can be used in a CSV-file
     * @throws SQLException if a database error occurs
     */
    public static String getCSVHeader(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        String ret = "";
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            ret += "'" +  rsmd.getColumnLabel(i) + "',";
        }
        return ret.substring(0, ret.length() - 1);
    }

    /**
     * Used to create the automatic indices (i.e. index for columns JOB_ID,TASK_INDEX) on a given table.
     *
     * @param table the table to be indexed
     * @throws SQLException if a database error occurs
     */
    public static void automaticIndex(String table) throws SQLException {
        System.out.println("Creating automatic index for table " + table);
        if (ResourceBundle.getBundle("resources").getString("auto-create-index").equals("1")) {
            createIndex(table, "JOB_ID", "TASK_INDEX");
        }
    }

    /**
     * Creates an index on the given columns of the given table.
     *
     * @param table the table to be indexed
     * @param columns the columns used in the index
     * @throws SQLException if a database error occurs
     */
    public static void createIndex(String table, String... columns) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        if (columns.length == 0) return;

        String name = "idx_" + table + "_";
        String fields = "(";
        for (String s : columns) {
            name += s + "_";
            fields += s + ",";
        }
        name = name.substring(0, name.length() - 1);
        fields = fields.substring(0, fields.length() - 1) + ")";

        statement.execute("CREATE INDEX " + name + " ON " + table + " " + fields);

        statement.close();
        connection.close();
    }
}
