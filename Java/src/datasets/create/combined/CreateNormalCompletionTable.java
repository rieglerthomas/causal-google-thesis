package datasets.create.combined;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extracts all tasks which completed normally in the combined dataset.
 */
public class CreateNormalCompletionTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE combined_completion " +
                "SELECT DISTINCT JOB_ID,TASK_INDEX,1 as COMPLETED_NORMALLY " +
                "FROM task_events_filtered " +
                "WHERE EVENT_TYPE = 4");

        Misc.automaticIndex("combined_completion");

        statement.close();
        connection.close();
    }
}
