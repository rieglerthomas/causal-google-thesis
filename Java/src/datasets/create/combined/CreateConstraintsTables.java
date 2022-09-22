package datasets.create.combined;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extracts the "Number of initial constraints" and "Number of total constraints" variables for the combined dataset.
 */
public class CreateConstraintsTables {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE combined_constraints_initial " +
                "SELECT JOB_ID, TASK_INDEX, COUNT(*) AS NUMBER_OF_INITIAL_CONSTRAINTS " +
                "FROM task_constraints_filtered t1 " +
                "WHERE t1.TIME <= (SELECT MIN(TIME) FROM task_events_filtered t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX AND t2.EVENT_TYPE = 0) " +
                "GROUP BY JOB_ID, TASK_INDEX");

        statement.execute("CREATE TABLE combined_constraints_total " +
                "SELECT JOB_ID, TASK_INDEX, COUNT(*) AS NUMBER_OF_TOTAL_CONSTRAINTS " +
                "FROM task_constraints_filtered " +
                "GROUP BY JOB_ID, TASK_INDEX");

        Misc.automaticIndex("combined_constraints_initial");
        Misc.automaticIndex("combined_constraints_total");

        statement.close();
        connection.close();
    }
}
