package datasets.create.combined;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Joins all tables for the combined dataset.
 */
public class CreateCombinedDataset {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE combined_dataset " +
                "SELECT " +
                "combined_usage.JOB_ID AS JOB_ID, " +
                "combined_usage.TASK_INDEX AS TASK_INDEX, " +
                "combined_usage.RUNTIME AS RUNTIME," +
                "combined_initial.INITIAL_PRIORITY AS PRIORITY," +
                "combined_initial.INITIAL_SCHEDULING_CLASS AS SCHEDULING_CLASS," +
                "COALESCE(constraints_initial.NUMBER_OF_INITIAL_CONSTRAINTS, 0) AS NUMBER_OF_INITIAL_CONSTRAINTS," +
                "COALESCE(constraints_total.NUMBER_OF_TOTAL_CONSTRAINTS, 0) AS NUMBER_OF_TOTAL_CONSTRAINTS," +
                "combined_usage.NUMBER_OF_MACHINES AS NUMBER_OF_MACHINES," +
                "combined_initial.INITIAL_CPU_REQUEST AS INITIAL_CPU_REQUEST," +
                "combined_initial.INITIAL_MEMORY_REQUEST AS INITIAL_MEMORY_REQUEST, " +
                "combined_initial.INITIAL_DISK_SPACE_REQUEST AS INITIAL_DISK_SPACE_REQUEST, " +
                "combined_max.MAX_CPU_REQUEST AS MAX_CPU_REQUEST, " +
                "combined_max.MAX_MEMORY_REQUEST AS MAX_MEMORY_REQUEST, " +
                "combined_max.MAX_DISK_SPACE_REQUEST AS MAX_DISK_SPACE_REQUEST, " +
                "combined_initial.DIFFERENT_MACHINES_RESTRICTION AS DIFFERENT_MACHINES_RESTRICTION," +
                "COALESCE(combined_updates.NUMBER_OF_UPDATES, 0) AS NUMBER_OF_UPDATES," +
                "combined_fails_evicts.NUMBER_OF_EVICTS AS NUMBER_OF_EVICTS," +
                "combined_fails_evicts.NUMBER_OF_FAILS AS NUMBER_OF_FAILS," +
                "COALESCE(combined_restarts.NUMBER_OF_RESTARTS, 0) AS NUMBER_OF_RESTARTS," +
                "combined_usage.AVG_CPU_RATE AS MEAN_CPU_RATE," +
                "combined_usage.AVG_CANONICAL_MEMORY_USAGE AS MEAN_CANONICAL_MEMORY_USAGE," +
                "combined_usage.AVG_ASSIGNED_MEMORY_USAGE AS MEAN_ASSIGNED_MEMORY_USAGE," +
                "combined_usage.AVG_UNMAPPED_PAGE_CACHE AS MEAN_UNMAPPED_PAGE_CACHE," +
                "combined_usage.AVG_TOTAL_PAGE_CACHE AS MEAN_TOTAL_PAGE_CACHE," +
                "combined_usage.AVG_DISK_IO_TIME AS MEAN_DISK_IO_TIME," +
                "combined_usage.AVG_LOCAL_DISK_SPACE_USAGE AS MEAN_LOCAL_DISK_SPACE_USAGE," +
                "combined_usage.AVG_CPI AS MEAN_CPI," +
                "combined_usage.AVG_MAI AS MEAN_MAI," +
                "combined_usage.AVG_MAXIMUM_CPU_RATE AS MEAN_MAXIMUM_CPU_RATE," +
                "combined_usage.AVG_MAXIMUM_MEMORY_USAGE AS MEAN_MAXIMUM_MEMORY_USAGE," +
                "combined_usage.AVG_MAXIMUM_DISK_IO_TIME AS MEAN_MAXIMUM_DISK_IO_TIME," +
                "COALESCE(combined_completion.COMPLETED_NORMALLY, 0) AS COMPLETED_NORMALLY " +
                "FROM combined_usage " +
                "LEFT JOIN combined_initial ON combined_usage.JOB_ID = combined_initial.JOB_ID AND combined_usage.TASK_INDEX = combined_initial.TASK_INDEX " +
                "LEFT JOIN combined_max ON combined_usage.JOB_ID = combined_max.JOB_ID AND combined_usage.TASK_INDEX = combined_max.TASK_INDEX " +
                "LEFT JOIN combined_constraints_initial constraints_initial ON combined_usage.JOB_ID = constraints_initial.JOB_ID AND combined_usage.TASK_INDEX = constraints_initial.TASK_INDEX " +
                "lEFT JOIN combined_constraints_total constraints_total ON combined_usage.JOB_ID = constraints_total.JOB_ID AND combined_usage.TASK_INDEX = constraints_total.TASK_INDEX " +
                "LEFT JOIN combined_updates ON combined_usage.JOB_ID = combined_updates.JOB_ID AND combined_usage.TASK_INDEX = combined_updates.TASK_INDEX " +
                "LEFT JOIN combined_fails_evicts ON combined_usage.JOB_ID = combined_fails_evicts.JOB_ID AND combined_usage.TASK_INDEX = combined_fails_evicts.TASK_INDEX " +
                "LEFT JOIN combined_restarts ON combined_usage.JOB_ID = combined_restarts.JOB_ID AND combined_usage.TASK_INDEX = combined_restarts.TASK_INDEX " +
                "LEFT JOIN combined_completion ON combined_usage.JOB_ID = combined_completion.JOB_ID AND combined_usage.TASK_INDEX = combined_completion.TASK_INDEX");

        Misc.automaticIndex("combined_dataset");

        statement.close();
        connection.close();
    }
}
