package datasets.create.combined;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extracts the "Runtime", "Number of machines" and the various usage metrics for the combined dataset.
 */
public class CreateCombinedUsageTable {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE combined_usage " +
                "SELECT " +
                "JOB_ID, " +
                "TASK_INDEX, " +
                "(MAX(END_TIME) - MIN(START_TIME)) / 1000000 AS RUNTIME, " +
                "AVG(CPU_RATE) AS AVG_CPU_RATE, " +
                "AVG(CANONICAL_MEMORY_USAGE) AS AVG_CANONICAL_MEMORY_USAGE, " +
                "AVG(ASSIGNED_MEMORY_USAGE) AS AVG_ASSIGNED_MEMORY_USAGE, " +
                "AVG(UNMAPPED_PAGE_CACHE) AS AVG_UNMAPPED_PAGE_CACHE, " +
                "AVG(TOTAL_PAGE_CACHE) AS AVG_TOTAL_PAGE_CACHE, " +
                "AVG(DISK_IO_TIME) AS AVG_DISK_IO_TIME, " +
                "AVG(LOCAL_DISK_SPACE_USAGE) AS AVG_LOCAL_DISK_SPACE_USAGE, " +
                "AVG(CPI) AS AVG_CPI, " +
                "AVG(MAI) AS AVG_MAI, " +
                "AVG(MAXIMUM_MEMORY_USAGE) AS AVG_MAXIMUM_MEMORY_USAGE, " +
                "AVG(MAXIMUM_CPU_RATE) AS AVG_MAXIMUM_CPU_RATE, " +
                "AVG(MAXIMUM_DISK_IO_TIME) AS AVG_MAXIMUM_DISK_IO_TIME, " +
                "COUNT(DISTINCT MACHINE_ID) AS NUMBER_OF_MACHINES " +
                "FROM task_usage_filtered " +
                "GROUP BY JOB_ID, TASK_INDEX");

        Misc.automaticIndex("combined_usage");

        statement.close();
        connection.close();
    }
}
