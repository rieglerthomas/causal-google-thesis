package datasets;

import datasets.analysis.Combinations;
import datasets.analysis.RunningTasks;
import datasets.create.basic.CreateCompletenessTable;
import datasets.create.basic.CreateDataTables;
import datasets.create.basic.CreateExclusionTable;
import datasets.create.general.*;
import datasets.create.usage.CreateCompletenessHundredTable;
import datasets.csv.CSVCombinedDataset;
import datasets.csv.CSVGeneralDataset;
import datasets.csv.CSVTaskUsage100;

import java.io.IOException;
import java.sql.SQLException;

/**
 * This class can be used to get to the final three datasets used in the thesis (in csv + SQL).
 * It's also possible to call the main methods of each of the given classes separately.
 */
public class Main {
    public static void main(String[] args) throws SQLException, IOException {
        // Basic
        CreateDataTables.main(null); // moving the data from csv-files into an SQL database
        CreateExclusionTable.main(null); // creating a table of tasks which should not be considered for various reasons
        CreateCompletenessTable.main(null); // creating a table which contains information on how many task usage rows exist per task and how many of them are incomplete

        // Filtering out records from the SQL tables which are not needed for the combined dataset, this improves performance
        datasets.create.combined.CreateCompletenessSeventyTable.main(null); // extracts those tasks from the completeness table which have 70% complete usage data
        datasets.create.combined.CreateTaskConstraintsFiltered.main(null); // filters out all rows of the task constraints table which are not in the previously constructed completeness table
        datasets.create.combined.CreateTaskEventsFiltered.main(null); // filters out all rows of the task events table which are not in the previously constructed completeness table
        datasets.create.combined.CreateTaskUsageFiltered.main(null); // filters out all rows of the task usage table which are not in the previously constructed completeness table

        // Creating the task usage dataset (as given in the thesis, 100% completeness)
        CreateCompletenessHundredTable.main(null); // extracts those tasks from the completeness table which have 100% complete usage data
        CSVTaskUsage100.main(null); // converts task usage data for complete tasks into a csv, this dataset is used in the thesis

        // Creating the general task usage information dataset (as given in the thesis)
        CreateInitialTable.main(null); // extracts the initial information from the task events table (e.g. initial CPU request)
        CreateMaxTable.main(null); // extracts the maximum information from the task events table (e.g. maximum CPU request)
        CreateNormalCompletionTable.main(null); // contains all tasks which completed normally
        CreateRestartsTable.main(null); // extracts the number of restarts for each task
        CreateUpdatesTable.main(null); // extracts the number of updates for each task
        CreateConstraintsTables.main(null); // extracts the number of total and initial constraints for each task
        CreateFailsEvictsTable.main(null); // extracts the number of evicts and fails for each task
        CreateUsageInfoTable.main(null); // extracts the runtime and the number of machines for each task
        CreateGeneralDataset.main(null); // joins all previous tables
        CSVGeneralDataset.main(null); // converts the joined dataset into a csv, this dataset is used in the thesis

        // Creating the combined dataset (as given in the thesis)
        datasets.create.combined.CreateInitialTable.main(null); // same as for the general dataset
        datasets.create.combined.CreateMaxTable.main(null); // same as for the general dataset
        datasets.create.combined.CreateConstraintsTables.main(null); // same as for the general dataset
        datasets.create.combined.CreateRestartsTable.main(null); // same as for the general dataset
        datasets.create.combined.CreateUpdatesTable.main(null); // same as for the general dataset
        datasets.create.combined.CreateCombinedUsageTable.main(null); // contains the runtime, number of machines and the mean of the usage metrics
        datasets.create.combined.CreateFailsEvictsTable.main(null); // same as for the general dataset
        datasets.create.combined.CreateNormalCompletionTable.main(null); // same as for the general dataset
        datasets.create.combined.CreateCombinedDataset.main(null); // joins all previous tables
        CSVCombinedDataset.main(null); // converts the joined dataset into a csv, this dataset is used in the thesis

        // Create the combinations as seen in the discussion part of the thesis
        Combinations.main(null); // exemplary combinations for the runtime

        // Create the data for the two charts in the thesis
        RunningTasks.main(null);
    }
}
