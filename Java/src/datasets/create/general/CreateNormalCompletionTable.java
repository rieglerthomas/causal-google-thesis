package datasets.create.general;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extracts all tasks which completed normally in the general dataset.
 */
public class CreateNormalCompletionTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE general_completion " +
                "SELECT DISTINCT JOB_ID,TASK_INDEX,1 as COMPLETED_NORMALLY " +
                "FROM task_events " +
                "WHERE EVENT_TYPE = 4");

        Misc.automaticIndex("general_completion");

        statement.close();
        connection.close();
    }
}
