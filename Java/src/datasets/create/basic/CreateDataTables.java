package datasets.create.basic;

import datasets.utility.ConnectionWrapper;
import datasets.utility.Misc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * Inserts all files of the Google cluster data 2011 into the SQL database using multiple threads.
 */
public class CreateDataTables {

    private final static int NUM_OF_THREADS;
    private final static ExecutorService THREADS;

    private final static String FILE_DIRECTORY;

    private static final int BATCH_SIZE_LIMIT;

    static {
        NUM_OF_THREADS = Integer.parseInt(ResourceBundle.getBundle("resources").getString("threads"));
        BATCH_SIZE_LIMIT = Integer.parseInt(ResourceBundle.getBundle("resources").getString("default-batch-size"));

        THREADS = Executors.newFixedThreadPool(NUM_OF_THREADS);
        FILE_DIRECTORY = ResourceBundle.getBundle("resources").getString("data-directory");
    }

    public static void main(String[] args) throws SQLException {
        createTables();

        // Remove directory if it should not be inserted
        String[] directories = {"task_usage", "task_events", "task_constraints", "job_events", "machine_attributes", "machine_events"};
        if (ResourceBundle.getBundle("resources").getString("create-unused-tables").equals("0")) {
            directories = new String[]{"task_usage", "task_events", "task_constraints"};
        }
        for (String directory : directories) {
            insertData(directory);
        }
        THREADS.shutdown();
        try {
            THREADS.awaitTermination(1L, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Main thread interrupted while awaiting termination for sql.create tables!", e);
        }

        for (String s : directories) {
            Misc.automaticIndex(s);
        }
    }

    private static void createTables() throws SQLException {
        System.out.println("Initializing database");
        Connection connection = ConnectionWrapper.getConnection();
        Statement statement = connection.createStatement();

        boolean createUnused = ResourceBundle.getBundle("resources").getString("create-unused-tables").equals("1");

        if (createUnused) statement.execute("CREATE TABLE IF NOT EXISTS job_events (" +
                "TIME BIGINT UNSIGNED NOT NULL," +
                "MISSING_INFO TINYINT," +
                "JOB_ID BIGINT NOT NULL," +
                "EVENT_TYPE TINYINT NOT NULL," +
                "USER VARCHAR(44)," +
                "SCHEDULING_CLASS TINYINT," +
                "JOB_NAME VARCHAR(44)," +
                "LOGICAL_JOB_NAME VARCHAR(44)" +
                ");");
        statement.execute("CREATE TABLE IF NOT EXISTS task_events (" +
                "TIME BIGINT UNSIGNED NOT NULL," +
                "MISSING_INFO TINYINT," +
                "JOB_ID BIGINT NOT NULL," +
                "TASK_INDEX MEDIUMINT NOT NULL," +
                "MACHINE_ID BIGINT," +
                "EVENT_TYPE TINYINT NOT NULL," +
                "USER VARCHAR(44)," +
                "SCHEDULING_CLASS TINYINT," +
                "PRIORITY TINYINT," +
                "CPU_REQUEST FLOAT," +
                "MEMORY_REQUEST FLOAT," +
                "DISK_SPACE_REQUEST FLOAT," +
                "DIFFERENT_MACHINES_RESTRICTION BIT" +
                ");");
        if (createUnused) statement.execute("CREATE TABLE IF NOT EXISTS machine_events (" +
                "TIME BIGINT UNSIGNED NOT NULL," +
                "MACHINE_ID BIGINT NOT NULL," +
                "EVENT_TYPE TINYINT NOT NULL," +
                "PLATFORM_ID VARCHAR(44)," +
                "CPUS FLOAT," +
                "MEMORY FLOAT" +
                ");");
        if (createUnused) statement.execute("CREATE TABLE IF NOT EXISTS machine_attributes (" +
                "TIME BIGINT UNSIGNED NOT NULL," +
                "MACHINE_ID BIGINT NOT NULL," +
                "ATTRIBUTE_NAME VARCHAR(44) NOT NULL," +
                "ATTRIBUTE_VALUE VARCHAR(44)," +
                "ATTRIBUTE_DELETED BIT NOT NULL" +
                ");");
        statement.execute("CREATE TABLE IF NOT EXISTS task_constraints (" +
                "TIME BIGINT UNSIGNED NOT NULL," +
                "JOB_ID BIGINT NOT NULL," +
                "TASK_INDEX MEDIUMINT NOT NULL," +
                "COMPARISON_OPERATOR TINYINT NOT NULL," +
                "ATTRIBUTE_NAME VARCHAR(44) NOT NULL," +
                "ATTRIBUTE_VALUE VARCHAR(44)" +
                ");");
        statement.execute("CREATE TABLE IF NOT EXISTS task_usage (" +
                "START_TIME BIGINT UNSIGNED NOT NULL," +
                "END_TIME BIGINT UNSIGNED NOT NULL," +
                "JOB_ID BIGINT NOT NULL," +
                "TASK_INDEX MEDIUMINT NOT NULL," +
                "MACHINE_ID BIGINT NOT NULL," +
                "CPU_RATE FLOAT," +
                "CANONICAL_MEMORY_USAGE FLOAT," +
                "ASSIGNED_MEMORY_USAGE FLOAT," +
                "UNMAPPED_PAGE_CACHE FLOAT," +
                "TOTAL_PAGE_CACHE FLOAT," +
                "MAXIMUM_MEMORY_USAGE FLOAT," +
                "DISK_IO_TIME FLOAT," +
                "LOCAL_DISK_SPACE_USAGE FLOAT," +
                "MAXIMUM_CPU_RATE FLOAT," +
                "MAXIMUM_DISK_IO_TIME FLOAT," +
                "CPI FLOAT," +
                "MAI FLOAT," +
                "SAMPLE_PORTION BIT," +
                "AGGREGATION_TYPE BIT," +
                "SAMPLED_CPU_USAGE FLOAT" +
                ");");
        statement.close();
        connection.close();
    }

    private static void insertData(String directory) {
        int parts = 500;
        if (directory.startsWith("machine")) {
            parts = 1;
        }

        System.out.println("Processing directory " + directory);

        for (int i = 0; i < parts; i++) {
            THREADS.execute(new InsertTask(directory, i));
        }
    }

    /**
     * Inserts one file of the Google cluster data 2011 into an SQL database.
     *
     * It is assumed that these files exist in a certain directory on your hard drive
     * and that you did not change the original files (name, still compressed).
     */
    private static class InsertTask implements Runnable {

        private final String directory;
        private final int fileNo;
        private int[] columnTypes;

        /**
         * Creates a new insertion task for file with number fileNo of one of the Google cluster data 2011 directories (e.g. task_usage).
         *
         * @param directory the directory from the Google Cluster data 2011
         * @param fileNo number of the file to be inserted (0 <= fileNo < 500)
         */
        public InsertTask(String directory, int fileNo) {
            this.directory = directory;
            this.fileNo = fileNo;
        }
        private void insertData() throws SQLException, IOException {
            Connection connection = ConnectionWrapper.getConnection();
            int parts = 500;
            if (directory.startsWith("machine")) {
                parts = 1;
            }

            String strParts = "" + parts;
            while (strParts.length() < 5) {
                strParts = "0" + strParts;
            }

            String number = "" + fileNo;
            while (number.length() < 5) {
                number = "0" + number;
            }
            System.out.println("Processing file " + fileNo + " of directory " + directory);
            long startFile = System.currentTimeMillis();
            Path source = Paths.get(  FILE_DIRECTORY + "/" + directory + "/part-" + number + "-of-" + strParts + ".csv.gz");

            try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(source.toFile()))) {
                BufferedReader br = new BufferedReader(new InputStreamReader(gis));

                String temp;
                PreparedStatement statement = connection.prepareStatement("INSERT INTO " + directory + " VALUES (" + getPlaceHolderForPreparedStatement() + ")");
                long counter = 0;
                while ((temp = br.readLine()) != null) {
                    String[] strs = temp.split(",", -1);
                    setValues(statement, strs);
                    counter++;
                    if (counter == BATCH_SIZE_LIMIT) {
                        statement.executeBatch();
                        counter = 0;
                    }
                }
                statement.executeBatch();
                statement.close();
                connection.close();
                System.out.println("File " + fileNo + " processed in " + ((System.currentTimeMillis() - startFile) / 1000) + " seconds");
            }
        }

        private void setValues(PreparedStatement ps, String[] values) throws SQLException {
            for (int i = 0; i < columnTypes.length; i++) {
                if (values[i].isEmpty()) {
                    ps.setNull(i + 1, columnTypes[i]);
                }
                if (columnTypes[i] == Types.INTEGER || columnTypes[i] == Types.BIGINT || columnTypes[i] == Types.SMALLINT || columnTypes[i] == Types.TINYINT || columnTypes[i] == Types.BIT) {
                    try {
                        if (!values[i].isEmpty())
                            ps.setLong(i + 1, Long.parseLong(values[i]));
                    } catch (NumberFormatException e) {
                        System.err.println("Cannot convert '" + values[i] + "' to long! Skipping record!");
                        ps.clearParameters();
                        return;
                    }
                } else if (columnTypes[i] == Types.VARCHAR) {
                    if (!values[i].isEmpty())
                        ps.setString(i + 1, values[i]);
                } else if (columnTypes[i] == Types.FLOAT || columnTypes[i] == Types.REAL) {
                    if (!values[i].isEmpty())
                        ps.setFloat(i + 1, Float.parseFloat(values[i]));
                } else {
                    throw new RuntimeException("Unknown datatype " + columnTypes[i]);
                }
            }
            ps.addBatch();
        }

        private String getPlaceHolderForPreparedStatement() {
            int numValues = columnTypes.length;
            String ret = "";
            for (int i = 0; i < numValues; i++) {
                ret += "?,";
            }
            return ret.substring(0, ret.length() - 1);
        }

        private void fillColumnTypesArray() throws SQLException {
            Connection connection = ConnectionWrapper.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM " + directory + " WHERE 1 = 0");
            ResultSetMetaData rsmd = resultSet.getMetaData();
            columnTypes = new int[rsmd.getColumnCount()];
            for (int i = 0; i < columnTypes.length; i++) {
                columnTypes[i] = rsmd.getColumnType(i + 1);
            }
            resultSet.close();
            statement.close();
            connection.close();
        }

        @Override
        public void run() {
            try {
                fillColumnTypesArray();
                insertData();
            } catch (SQLException e) {
                throw new RuntimeException("SQL exception during insertion for file number " + fileNo + " of directory '" + directory + "'", e);
            } catch (IOException e) {
                throw new RuntimeException("I/O exception during insertion for file number " + fileNo + " of directory '" + directory + "'", e);
            }
        }
    }
}
