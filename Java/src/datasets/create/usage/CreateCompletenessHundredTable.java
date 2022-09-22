package datasets.create.usage;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Creates the table which contains all tasks which have 100% of complete task usage information (as used in the thesis).
 */
public class CreateCompletenessHundredTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE tasks_completeness_100 " +
                "SELECT JOB_ID, TASK_INDEX FROM tasks_completeness WHERE (NUMBER_OF_ROWS - INCOMPLETE_ROWS) / NUMBER_OF_ROWS >= 1.0");

        Misc.automaticIndex("tasks_completeness_100");

        statement.close();
        connection.close();
    }
}
