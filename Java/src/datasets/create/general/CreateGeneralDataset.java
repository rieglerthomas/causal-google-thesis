package datasets.create.general;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Joins all tables for the general dataset.
 */
public class CreateGeneralDataset {
    public static void main(String[] args) throws SQLException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        statement.execute("CREATE TABLE general_dataset " +
                "SELECT " +
                "general_initial.JOB_ID AS JOB_ID, " +
                "general_initial.TASK_INDEX AS TASK_INDEX, " +
                "general_initial.SCHEDULING_CLASS AS SCHEDULING_CLASS, " +
                "general_initial.PRIORITY AS PRIORITY, " +
                "general_initial.INITIAL_CPU_REQUEST AS INITIAL_CPU_REQUEST, " +
                "general_initial.INITIAL_MEMORY_REQUEST AS INITIAL_MEMORY_REQUEST, " +
                "general_initial.INITIAL_DISK_SPACE_REQUEST AS INITIAL_DISK_SPACE_REQUEST, " +
                "general_initial.DIFFERENT_MACHINES_RESTRICTION AS DIFFERENT_MACHINES_RESTRICTION, " +
                "general_usage.RUNTIME AS RUNTIME, " +
                "general_usage.NUMBER_OF_MACHINES AS NUMBER_OF_MACHINES, " +
                "COALESCE(constraints_initial.NUMBER_OF_INITIAL_CONSTRAINTS, 0) AS NUMBER_OF_INITIAL_CONSTRAINTS, " +
                "COALESCE(constraints_total.NUMBER_OF_TOTAL_CONSTRAINTS, 0) AS NUMBER_OF_TOTAL_CONSTRAINTS, " +
                "evicts_fails.NUMBER_OF_EVICTS AS NUMBER_OF_EVICTS, " +
                "evicts_fails.NUMBER_OF_FAILS AS NUMBER_OF_FAILS, " +
                "COALESCE(completion.COMPLETED_NORMALLY, 0) AS COMPLETED_NORMALLY, " +
                "general_max.MAX_CPU_REQUEST AS MAX_CPU_REQUEST, " +
                "general_max.MAX_MEMORY_REQUEST AS MAX_MEMORY_REQUEST, " +
                "general_max.MAX_DISK_SPACE_REQUEST AS MAX_DISK_SPACE_REQUEST, " +
                "COALESCE(updates.NUMBER_OF_UPDATES, 0) AS NUMBER_OF_UPDATES, " +
                "COALESCE(restarts.NUMBER_OF_RESTARTS, 0) AS NUMBER_OF_RESTARTS " +
                "FROM general_initial " +
                "LEFT JOIN general_usage ON general_initial.JOB_ID = general_usage.JOB_ID AND general_initial.TASK_INDEX = general_usage.TASK_INDEX " +
                "LEFT JOIN general_constraints_initial constraints_initial ON constraints_initial.JOB_ID = general_initial.JOB_ID AND constraints_initial.TASK_INDEX = general_initial.TASK_INDEX " +
                "LEFT JOIN general_constraints_total constraints_total ON constraints_total.JOB_ID = general_initial.JOB_ID AND constraints_total.TASK_INDEX = general_initial.TASK_INDEX " +
                "LEFT JOIN general_fails_evicts evicts_fails ON evicts_fails.JOB_ID = general_initial.JOB_ID AND evicts_fails.TASK_INDEX = general_initial.TASK_INDEX " +
                "LEFT JOIN general_completion completion ON completion.JOB_ID = general_initial.JOB_ID AND completion.TASK_INDEX = general_initial.TASK_INDEX " +
                "LEFT JOIN general_max ON general_max.JOB_ID = general_initial.JOB_ID AND general_max.TASK_INDEX = general_initial.TASK_INDEX " +
                "LEFT JOIN general_updates updates ON updates.JOB_ID = general_initial.JOB_ID AND updates.TASK_INDEX = general_initial.TASK_INDEX " +
                "LEFT JOIN general_restarts restarts ON restarts.JOB_ID = general_initial.JOB_ID AND restarts.TASK_INDEX = general_initial.TASK_INDEX ");

        Misc.automaticIndex("general_dataset");

        statement.close();
        connection.close();
    }
}
