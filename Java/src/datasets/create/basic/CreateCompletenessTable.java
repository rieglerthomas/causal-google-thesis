package datasets.create.basic;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * Creates the task completeness table which contains information on how many task usage rows exist for one task
 * and the number of rows which do not miss any usage information.
 */
public class CreateCompletenessTable {

    private static final ConcurrentHashMap<String, Integer> NULL_MAP;
    private static final ConcurrentHashMap<String, Integer> COUNT_MAP;
    private static final ExecutorService THREADS;
    private static final int NUM_OF_THREADS;

    private final static String FILE_DIRECTORY;
    private final static int BATCH_SIZE_LIMIT;

    static {
        NUM_OF_THREADS = Integer.parseInt(ResourceBundle.getBundle("resources").getString("threads"));;
        BATCH_SIZE_LIMIT = Integer.parseInt(ResourceBundle.getBundle("resources").getString("default-batch-size"));

        NULL_MAP = new ConcurrentHashMap<>();
        COUNT_MAP = new ConcurrentHashMap<>();
        THREADS = Executors.newFixedThreadPool(NUM_OF_THREADS);
        FILE_DIRECTORY = ResourceBundle.getBundle("resources").getString("data-directory");
    }

    public static void main(String[] args) throws SQLException {
        createTable();
        insert();
        Misc.automaticIndex("tasks_completeness");
    }

    private static void createTable() throws SQLException {
        Connection con = ConnectionWrapper.getConnection();
        Statement statement = con.createStatement();

        statement.execute("CREATE TABLE tasks_completeness (" +
                "JOB_ID BIGINT NOT NULL," +
                "TASK_INDEX MEDIUMINT NOT NULL," +
                "NUMBER_OF_ROWS MEDIUMINT NOT NULL," +
                "INCOMPLETE_ROWS MEDIUMINT NOT NULL" +
                ")");

        statement.close();
        con.close();
    }

    private static void insert() throws SQLException {
        for (int i = 0; i < 500; i++) {
            THREADS.execute(new RowCheckTask(i));
        }

        THREADS.shutdown();
        try {
            if (!THREADS.awaitTermination(1L, TimeUnit.DAYS)) {
                throw new RuntimeException("Threads did not terminate before timeout ended!");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Main thread interrupted before all threads finished the row check", e);
        }


        Connection connection = ConnectionWrapper.getConnection();
        PreparedStatement ps = connection.prepareStatement("INSERT INTO tasks_completeness  VALUES (?,?,?,?)");

        Iterator<String> keySet = COUNT_MAP.keySet().iterator();
        long count = 0;

        while (keySet.hasNext()) {
            String key = keySet.next();
            String[] keyParts = key.split(":");
            Integer counts = COUNT_MAP.get(key);
            Integer nulls = NULL_MAP.get(key);
            ps.setLong(1, Long.parseLong(keyParts[0]));
            ps.setLong(2, Long.parseLong(keyParts[1]));
            ps.setInt(3, counts == null ? 0 : counts);
            ps.setInt(4, nulls == null ? 0 : nulls);
            ps.addBatch();
            count++;
            if (count == BATCH_SIZE_LIMIT) {
                System.out.println("Executing batch!");
                ps.executeBatch();
                count = 0;
            }
        }
        System.out.println("Executing batch!");
        ps.executeBatch();
        ps.close();
        connection.close();
    }

    /**
     * Counts number of total and incomplete rows for one task usage file.
     * It is assumed that all task usage files exist in some directory and are not changed.
     */
    private static class RowCheckTask implements Runnable {

        private final int fileNo;

        /**
         * Creates a task which counts the number of total and incomplete rows for the task usage file with number fileNo.
         *
         * @param fileNo the file to execute the task on
         */
        public RowCheckTask(int fileNo) {
            this.fileNo = fileNo;
        }

        @Override
        public void run() {
            try {
                count();
            } catch (IOException e) {
                throw new RuntimeException("I/O exception during insertion for file number " + fileNo + " of directory 'task_usage'", e);
            }
        }

        private void count() throws IOException {
            String number = "" + fileNo;
            while (number.length() < 5) {
                number = "0" + number;
            }
            System.out.println("Processing file " + fileNo + " of directory task_usage");
            long startFile = System.currentTimeMillis();
            Path source = Paths.get(FILE_DIRECTORY + "/task_usage/part-" + number + "-of-00500.csv.gz");

            try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(source.toFile()))) {
                BufferedReader br = new BufferedReader(new InputStreamReader(gis));

                String temp;
                while ((temp = br.readLine()) != null) {
                    String[] strs = temp.split(",", -1);
                    String key = strs[2] + ":" + strs[3];
                    COUNT_MAP.merge(key, 1, Integer::sum);
                    for (int i = 5; i < 17; i++) {
                        if (strs[i].isEmpty()) {
                            NULL_MAP.merge(key, 1, Integer::sum);
                            break;
                        }
                    }
                }

                System.out.println("File " + fileNo + " processed in " + ((System.currentTimeMillis() - startFile) / 1000) + " seconds");
            }
        }
    }
}
