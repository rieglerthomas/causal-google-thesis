package datasets.create.combined;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extracts the maximum resource request variables for the combined dataset.
 */
public class CreateMaxTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE combined_max " +
                "SELECT JOB_ID,TASK_INDEX,MAX(CPU_REQUEST) AS MAX_CPU_REQUEST,MAX(MEMORY_REQUEST) AS MAX_MEMORY_REQUEST,MAX(DISK_SPACE_REQUEST) AS MAX_DISK_SPACE_REQUEST " +
                "FROM task_events_filtered " +
                "GROUP BY JOB_ID, TASK_INDEX");

        Misc.automaticIndex("combined_max");

        statement.close();
        connection.close();
    }
}
