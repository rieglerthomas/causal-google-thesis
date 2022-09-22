package datasets.create.combined;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extracts the "Scheduling class", "Priority" and initial resource request variables for the combined dataset.
 */
public class CreateInitialTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE combined_initial " +
                "SELECT JOB_ID,TASK_INDEX, " +
                "SCHEDULING_CLASS AS INITIAL_SCHEDULING_CLASS, " +
                "PRIORITY AS INITIAL_PRIORITY, " +
                "CPU_REQUEST AS INITIAL_CPU_REQUEST, " +
                "MEMORY_REQUEST AS INITIAL_MEMORY_REQUEST, " +
                "DISK_SPACE_REQUEST AS INITIAL_DISK_SPACE_REQUEST," +
                "DIFFERENT_MACHINES_RESTRICTION " +
                "FROM task_events_filtered t1 " +
                "WHERE EVENT_TYPE = 0 AND " +
                "NOT EXISTS (SELECT * FROM task_events_filtered t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX AND t2.TIME < t1.TIME)");

        Misc.automaticIndex("combined_initial");

        statement.close();
        connection.close();
    }
}
