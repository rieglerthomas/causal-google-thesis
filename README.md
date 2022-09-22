# causal-google-thesis

## Structure
There are two main directories: Java and R. 

The Java directory contains an IntelliJ project which can be used to
insert the Google cluster data 2011 into a MySQL database and create the different, synthesized datasets (as seen in the thesis).
Furthermore, it provides functionality for determining the effect of causal relationships as done in the discussion part of the thesis
and for creating the data for the two charts (evolution of running tasks) given in the thesis. 
The `Main` class found in the `datasets` package can be used to go through the whole process of creating the SQL database and the synthesized datasets without running the separate classes. In order for the IntelliJ project to work, one needs to edit the `resources.properties` file found in the `src` directory.

The R directory contains an R-file which is used to create the two charts of the evolution of running tasks.

```bash
├───Java
│   └───src
│       └───datasets
│           ├───analysis
│           ├───create
│           │   ├───basic
│           │   ├───combined
│           │   ├───general
│           │   └───usage
│           ├───csv
│           └───utility
└───R
```
