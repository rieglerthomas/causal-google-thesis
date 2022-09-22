package datasets.create.general;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extracts the "Runtime" and "Number of machines" variables for the general dataset.
 */
public class CreateUsageInfoTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE general_usage " +
                "SELECT JOB_ID, TASK_INDEX, " +
                "(MAX(END_TIME) - MIN(START_TIME)) / 1000000 AS RUNTIME," +
                "COUNT(DISTINCT MACHINE_ID) AS NUMBER_OF_MACHINES " +
                "FROM task_usage " +
                "GROUP BY JOB_ID,TASK_INDEX");

        Misc.automaticIndex("general_usage");

        statement.close();
        connection.close();
    }
}
