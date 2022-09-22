package datasets.create.combined;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Creates the table which contains all tasks which have at least 70% of complete task usage information (as used in the thesis).
 */
public class CreateCompletenessSeventyTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE combined_completeness " +
                "SELECT JOB_ID, TASK_INDEX FROM tasks_completeness WHERE (NUMBER_OF_ROWS - INCOMPLETE_ROWS) / NUMBER_OF_ROWS >= 0.7");

        Misc.automaticIndex("combined_completeness");

        statement.close();
        connection.close();
    }
}
