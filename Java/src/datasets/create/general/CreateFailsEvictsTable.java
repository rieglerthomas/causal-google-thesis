package datasets.create.general;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extracts the "Number of evicts" and "Number of fails" variables for the general dataset.
 */
public class CreateFailsEvictsTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE general_fails_evicts " +
                "SELECT JOB_ID,TASK_INDEX,SUM(CASE WHEN EVENT_TYPE = 2 THEN 1 ELSE 0 END) AS NUMBER_OF_EVICTS, SUM(CASE WHEN EVENT_TYPE = 3 THEN 1 ELSE 0 END) AS NUMBER_OF_FAILS " +
                "FROM task_events " +
                "GROUP BY JOB_ID, TASK_INDEX");

        Misc.automaticIndex("general_fails_evicts");

        statement.close();
        connection.close();
    }
}
