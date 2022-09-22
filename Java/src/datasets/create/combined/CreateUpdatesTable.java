package datasets.create.combined;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extracts the "Number of updates" variable for the combined dataset.
 */
public class CreateUpdatesTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE combined_updates " +
                "SELECT JOB_ID,TASK_INDEX,COUNT(*) AS NUMBER_OF_UPDATES FROM " +
                "task_events_filtered WHERE EVENT_TYPE IN (7,8) GROUP BY JOB_ID, TASK_INDEX");

        Misc.automaticIndex("combined_updates");

        statement.close();
        connection.close();
    }
}
