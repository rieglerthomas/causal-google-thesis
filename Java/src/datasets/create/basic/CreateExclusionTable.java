package datasets.create.basic;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Creates a table of tasks which contains all tasks which should be excluded from the analysis.
 */
public class CreateExclusionTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("DROP TABLE IF EXISTS tasks_exclude");
        statement.execute("CREATE TABLE tasks_exclude (" +
                "JOB_ID BIGINT NOT NULL," +
                "TASK_INDEX MEDIUMINT NOT NULL," +
                "REASON VARCHAR(255) NOT NULL" +
                ")");

        statement.execute("CREATE TEMPORARY TABLE temp_tasks_initial " +
                "SELECT JOB_ID,TASK_INDEX, " +
                "CPU_REQUEST AS INITIAL_CPU_REQUEST, " +
                "MEMORY_REQUEST AS INITIAL_MEMORY_REQUEST, " +
                "DISK_SPACE_REQUEST AS INITIAL_DISK_SPACE_REQUEST " +
                "FROM task_events t1 " +
                "WHERE EVENT_TYPE = 0 AND " +
                "NOT EXISTS (SELECT * FROM task_events t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX AND t2.TIME < t1.TIME)");

        // Excludes all tasks which are missing initial request data
        statement.execute("INSERT INTO tasks_exclude SELECT JOB_ID,TASK_INDEX,'At least one initial resource request missing' as REASON " +
                "FROM temp_tasks_initial " +
                "WHERE INITIAL_CPU_REQUEST IS NULL OR INITIAL_MEMORY_REQUEST IS NULL OR INITIAL_DISK_SPACE_REQUEST IS NULL");

        // Excludes all task which did not finish before the end of the trace period
        statement.execute("INSERT INTO tasks_exclude " +
                "SELECT DISTINCT JOB_ID,TASK_INDEX,'No termination before trace end' as REASON " +
                "FROM task_events t1 " +
                "WHERE " +
                "EVENT_TYPE = 0 AND " +
                "NOT EXISTS (SELECT * FROM task_events t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX AND EVENT_TYPE = 0 AND t2.TIME > t1.TIME) AND " +
                "NOT EXISTS (SELECT * FROM task_events t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX AND EVENT_TYPE IN (2,3,4,5,6) AND t2.TIME > t1.TIME)");

        Misc.automaticIndex("tasks_exclude");

        statement.close();
        connection.close();
    }
}
