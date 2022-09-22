package datasets.create.combined;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Creates a filtered version of the task usage table containing only the tasks from the combined dataset.
 */
public class CreateTaskUsageFiltered {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE task_usage_filtered " +
                "SELECT * FROM task_usage t1 " +
                "WHERE EXISTS (SELECT * FROM combined_completeness t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX)");

        Misc.automaticIndex("task_usage_filtered");

        statement.close();
        connection.close();
    }
}
