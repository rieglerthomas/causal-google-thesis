package datasets.analysis;

import datasets.utility.ConnectionWrapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Creates the combinations as seen in the thesis using the given causes and the given target variable.
 */
public class Combinations {

    // TODO: Change causes and target variables as needed
    private static final String[] causes = {"NUMBER_OF_FAILS", "NUMBER_OF_MACHINES", "NUMBER_OF_TOTAL_CONSTRAINTS", "MEAN_LOCAL_DISK_SPACE_USAGE", "MAX_CPU_REQUEST"};
    private static final String targetVariable = "RUNTIME";

    private record Combination (byte[] intervals) {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Combination that = (Combination) o;

            return Arrays.equals(intervals, that.intervals);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(intervals);
        }
    }

    private record TaskCombination (Combination combination, double target) {}

    private static class Tasks {
        int numberOfTasks;
        double sumTarget;
    }

    public static void main(String[] args) throws SQLException, IOException {
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        final String dir = ResourceBundle.getBundle("resources").getString("output-directory") + "/";

        final String[] files = Arrays.stream(causes).map(s -> s + "-" + targetVariable + ".csv").toArray(String[]::new);

        for (String s : causes) {
            statement.execute("SELECT " + s + ", COUNT(*) AS NUMBER_OF_TASKS " +
                    "FROM combined_dataset t1 " +
                    "WHERE NOT EXISTS (SELECT * FROM tasks_exclude t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX) " +
                    "GROUP BY " + s + " " +
                    "ORDER BY " + s + " ASC " +
                    "INTO OUTFILE '" + dir + s + "-" + targetVariable + ".csv' " +
                    "FIELDS TERMINATED BY ',' " +
                    "LINES TERMINATED BY '\n'");
        }

        final String[] intervalFiles = Arrays.stream(files).map(s -> "iv-" + s).toArray(String[]::new);
        final int numberOfIntervals = Integer.parseInt(ResourceBundle.getBundle("resources").getString("intervals"));

        final HashMap<String, TaskCombination> taskCombinations = new HashMap<>();

        for (String s : files) {
            SplitIntoIntervals.splitInterval(dir, s, numberOfIntervals, dir);
        }

        for (String file : intervalFiles) {
            String column = file.split("-")[1];
            try (BufferedReader br = new BufferedReader(new FileReader(dir + file))) {
                br.readLine();

                String temp;
                while ((temp = br.readLine()) != null) {
                    String[] parts = temp.split(",");

                    ResultSet rs = statement.executeQuery("SELECT JOB_ID,TASK_INDEX," + targetVariable + " " +
                            "FROM combined_dataset t1 " +
                            "WHERE NOT EXISTS (SELECT * FROM tasks_exclude t2 WHERE t1.JOB_ID = t2.JOB_ID AND t1.TASK_INDEX = t2.TASK_INDEX) AND " + column + " >= " + parts[1] + " AND " + column + " <= " + parts[2]);

                    while (rs.next()) {
                        String key = rs.getString(1) + ":" + rs.getString(2);
                        if (!taskCombinations.containsKey(key)) {
                            taskCombinations.put(key,
                                    new TaskCombination(
                                            new Combination(new byte[causes.length]),
                                            rs.getDouble(3)));
                        }
                        TaskCombination combination = taskCombinations.get(key);
                        for (int i = 0; i < causes.length; i++) {
                            if (column.equals(causes[i])) {
                                combination.combination.intervals[i] = Byte.parseByte(parts[0]);
                            }
                        }
                    }

                    rs.close();
                }
            }
        }

        statement.execute("DROP TABLE IF EXISTS results_" + targetVariable.toLowerCase(Locale.ROOT));

        String stmt = "CREATE TABLE results_" + targetVariable.toLowerCase(Locale.ROOT) + "(";

        for (String s : causes) {
            stmt += s + " TINYINT NOT NULL,";
        }

        stmt += "NUMBER_OF_TASKS INT NOT NULL, AVG_" + targetVariable + " DOUBLE NOT NULL)";

        statement.execute(stmt);
        statement.close();

        HashMap<Combination, Tasks> combinations = new HashMap<>();

        for (String key : taskCombinations.keySet()) {
            TaskCombination taskCombination = taskCombinations.get(key);
            Combination combination = taskCombination.combination;
            Tasks tasks = combinations.get(combination);
            if (tasks == null) {
                tasks = new Tasks();
                combinations.put(combination, tasks);
            }
            tasks.numberOfTasks++;
            tasks.sumTarget += taskCombination.target;
        }

        stmt = "INSERT INTO results_" + targetVariable.toLowerCase(Locale.ROOT) + " VALUES (?,?,";
        for (int i = 0; i < causes.length; i++) {
            stmt += "?,";
        }
        stmt = stmt.substring(0, stmt.length() - 1);
        stmt += ")";

        PreparedStatement ps = connection.prepareStatement(stmt);

        for (Combination key : combinations.keySet()) {
            for (int i = 0; i < causes.length; i++) {
                ps.setByte(i + 1, key.intervals[i]);
            }

            Tasks tasks = combinations.get(key);
            ps.setInt(causes.length + 1, tasks.numberOfTasks);
            ps.setDouble(causes.length + 2, tasks.sumTarget / tasks.numberOfTasks);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();

        connection.close();

        for (String s : files) {
            new File(dir + s).delete();
        }

        for (String s : intervalFiles) {
            new File(dir + s).delete();
        }
    }
}
