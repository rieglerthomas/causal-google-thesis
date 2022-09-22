package datasets.create.combined;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extracts the "Number of restarts" variable for the combined dataset.
 */
public class CreateRestartsTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TEMPORARY TABLE temp_tasks_fails_kills_evicts " +
                "SELECT JOB_ID,TASK_INDEX,TIME,EVENT_TYPE " +
                "FROM task_events_filtered " +
                "WHERE EVENT_TYPE IN (2,3,5)");

        statement.execute("CREATE TEMPORARY TABLE TEMP_MIN_TIME " +
                "SELECT JOB_ID,TASK_INDEX,MIN(TIME) AS MIN_TIME " +
                "FROM temp_tasks_fails_kills_evicts " +
                "GROUP BY JOB_ID, TASK_INDEX");

        statement.execute("CREATE INDEX idx_temp ON TEMP_MIN_TIME (JOB_ID,TASK_INDEX)");

        statement.execute("CREATE TEMPORARY TABLE temp_tasks_submit_after " +
                "SELECT JOB_ID,TASK_INDEX,TIME " +
                "FROM task_events_filtered t1 " +
                "WHERE EVENT_TYPE = 0 AND " +
                "TIME >= (SELECT MIN_TIME FROM TEMP_MIN_TIME t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX)");

        statement.execute("CREATE TABLE combined_restarts " +
                "SELECT JOB_ID,TASK_INDEX,COUNT(*) AS NUMBER_OF_RESTARTS " +
                "FROM temp_tasks_fails_kills_evicts t1 " +
                "WHERE EXISTS (SELECT * FROM temp_tasks_submit_after t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX AND t2.TIME >= t1.TIME) " +
                "GROUP BY JOB_ID,TASK_INDEX");

        Misc.automaticIndex("combined_restarts");

        statement.close();
        connection.close();
    }
}
