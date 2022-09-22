package datasets.analysis;

import java.io.*;
import java.util.Arrays;

/**
 * Splits the input file into equal-length intervals according to the number of tasks variable.
 */
public class SplitIntoIntervals {

    public static void splitInterval(String inputDirectory, String inputFile, int numberOfIntervals, String outputDirectory) throws IOException {
        // [lower bound, upper bound, number of tasks]
        Double[][] intervals = new Double[numberOfIntervals][3];

        final int numberOfTasks = countNumberOfTasks(inputDirectory, inputFile);
        final int optLength = numberOfTasks / numberOfIntervals + 1;

        int currInterval = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(inputDirectory + inputFile))) {
            String temp;
            Double prevCategory = null;
            while ((temp = br.readLine()) != null) {
                String[] parts = temp.split(",");
                double category = Double.parseDouble(parts[0]);
                double tasks = Double.parseDouble(parts[1]);

                if (intervals[currInterval][2] != null) {
                    if (intervals[currInterval][2] + tasks > optLength) {
                        if (currInterval < numberOfIntervals - 1) {
                            intervals[currInterval][1] = prevCategory;
                            currInterval++;
                            intervals[currInterval][0] = category;
                            intervals[currInterval][2] = tasks;
                        } else {
                            intervals[currInterval][2] += tasks;
                        }
                    } else {
                        intervals[currInterval][2] += tasks;
                    }
                } else {
                    intervals[currInterval][0] = category;
                    intervals[currInterval][2] = tasks;
                }

                prevCategory = category;
            }
            intervals[currInterval][1] = prevCategory;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        File file = new File(outputDirectory + "iv-" + inputFile);
        if (file.exists()) file.delete();
        file.createNewFile();

        try (FileWriter fw  = new FileWriter(file)) {
            fw.write("INDEX,LOWER_BOUND,UPPER_BOUND,NUMBER_OF_TASKS\n");
            for (int i = 0; i < intervals.length; i++) {
                Double[] arr = intervals[i];
                try {
                    fw.write(i + "," + arr[0] + "," + arr[1] + "," + arr[2] + "\n");
                } catch (NullPointerException e) {
                    System.err.println("Interval " + i + " created null pointer: " + Arrays.toString(arr)) ;
                }
            }
        }
    }

    private static int countNumberOfTasks(String inputDirectory, String inputFile) {
        int ret = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(inputDirectory + inputFile))) {
            String temp;
            while ((temp = br.readLine()) != null) {
                String[] parts = temp.split(",");
                ret += Integer.parseInt(parts[1]);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }
}
