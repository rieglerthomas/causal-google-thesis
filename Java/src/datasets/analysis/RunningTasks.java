package datasets.analysis;

import datasets.utility.ConnectionWrapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ResourceBundle;

/**
 * This class is used to create the data for the two charts on the evolution of running tasks in certain datasets.
 */
public class RunningTasks {
    public static void main(String[] args) throws IOException, SQLException {
        final String outFile = ResourceBundle.getBundle("resources").getString("output-directory") + "/running-tasks.csv";

        int[] runningTasks = new int[2506200 + 1]; // the number of seconds in the trace period

        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        // For the combined dataset
        ResultSet rs = statement.executeQuery("SELECT MIN(START_TIME) / 1000000 AS START, MAX(END_TIME) / 1000000 AS END " +
                "FROM task_usage t1 " +
                "WHERE EXISTS (SELECT * FROM combined_completeness t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX) " +
                "AND NOT EXISTS (SELECT * FROM tasks_exclude t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX) " +
                "GROUP BY JOB_ID,TASK_INDEX");

        // For the task usage dataset
        /*ResultSet rs = statement.executeQuery("SELECT MIN(START_TIME) / 1000000 AS START, MAX(END_TIME) / 1000000 AS END " +
                "FROM task_usage t1 " +
                "WHERE EXISTS (SELECT * FROM tasks_completeness_100 t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX) " +
                "AND NOT EXISTS (SELECT * FROM tasks_exclude t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX)");*/

        while (rs.next()) {
            int start = (int) rs.getFloat(1);
            int end = (int) rs.getFloat(2);

            for (int i = start; i <= end; i++) {
                runningTasks[i]++;
            }
        }

        File file = new File(outFile);
        if (!file.createNewFile()) {
            throw new FileAlreadyExistsException("File '" + outFile + "' already exists!");
        }

        try (FileWriter fileWriter = new FileWriter(outFile)) {
            fileWriter.write("TIME,RUNNING_TASKS\n");
            for (int i = 0; i < runningTasks.length; i++) {
                fileWriter.write(i + "," + runningTasks[i] + "\n");
            }
        }
    }
}
