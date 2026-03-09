# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c("tidyverse", "data.table", "pointblank", "janitor"), # Packages that your targets need for their tasks.
  format = "qs", # Optionally set the default storage format. qs is fast.
  #
  # Pipelines that take a long time to run may benefit from
  # optional distributed computing. To use this capability
  # in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller that scales up to a maximum of two workers
  # which run as local R processes. Each worker launches when there is work
  # to do and exits if 60 seconds pass with no tasks to run.
  #
  #   controller = crew::crew_controller_local(workers = 2, seconds_idle = 60)
  #
  # Alternatively, if you want workers to run on a high-performance computing
  # cluster, select a controller from the {crew.cluster} package.
  # For the cloud, see plugin packages like {crew.aws.batch}.
  # The following example is a controller for Sun Grid Engine (SGE).
  #
  #   controller = crew.cluster::crew_controller_sge(
  #     # Number of workers that the pipeline can scale up to:
  #     workers = 10,
  #     # It is recommended to set an idle time so workers can shut themselves
  #     # down if they are not running tasks.
  #     seconds_idle = 120,
  #     # Many clusters install R as an environment module, and you can load it
  #     # with the script_lines argument. To select a specific verison of R,
  #     # you may need to include a version string, e.g. "module load R/4.3.2".
  #     # Check with your system administrator if you are unsure.
  #     script_lines = "module load R"
  #   )
  #
  # Set other options as needed.
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source()
# tar_source("other_functions.R") # Source other scripts as needed.

# Replace the target list below with your own:
list(
  tar_target(
    name = patients,
    command = readr::read_csv(unz("data.zip", "data-fixed/patients.csv")) |>
      setDT() |>
      setkey(id) |>
      janitor::remove_empty(quiet = FALSE) |>
      janitor::remove_constant(quiet = FALSE),
    format = "qs" # Efficient storage for general data objects.
  ),
  tar_target(
    name=checks,
    command=patients |>
      create_agent(label = "Agent check at your service") |>
      col_vals_between(
        where(is.Date),
        as.Date("1900-01-01"),
        as.Date(Sys.Date()),
        na_pass = TRUE,
        label = "Is the data between today and 1900?"
      ) |>
      col_vals_gte(
        deathdate,
        vars(birthdate),
        na_pass = TRUE,
        label = "Did the person die before he was born?"
      ) |>
      col_vals_regex(
        ssn, # This variable was described in ECS1!
        "[0-9]{3}-[0-9]{2}-[0-9]{4}$",
        label = "Make sure that the personnummer follows the correct format"
      ) |>
      col_is_integer(
        id,
        label = "Look at if the data id is a whole integer and not continous"
      ) |>
      col_vals_in_set(
        marital, # Chosen column
        set = c("S", "M", "D", "W"), # See if only these values are present
        na_pass = TRUE,
        label = "Are there any anomalies in the marital column?"
      ) |>
      col_vals_in_set(
        gender,
        set = c("M", "F"),
        na_pass = TRUE,
        label = "Let's see if we find any genders other than male and female"
      ) |>
      col_vals_gte(
        birthdate,
        Sys.Date() - lubridate::years(130),
        na_pass = TRUE,
        label = "Is the person older than 130 (probably dead)?"
      ) |>
      interrogate()
  ),
  # Moving onto the validation report which was supposed to come out in a html file
  tar_target(
    name = validation_report,
    command = export_report(checks, "patient_validation.html") # Use the .html to specify the html format 
  ),
  # Lets factorize what we're already working with
  tar_target(
    name = patients_cleaned, # new name
    command = { # Command to do multiple things

      # The marital factoring
      patients[,
      marital := factor(marital,
      levels = c("S", "M", "D", "W"),
      levels = c("Single", "Married", "Divorced", "Widowed")
      )
      ]

      # Gender can be refactored same as the marital status
      patients[,
      gender := factor(
        gender,
        levels = c("M", "F"),
        labels = c("Male", "Female")
      )
      ]
      # Re define the rest more quickly so we can just use their original names as factors
      patients[,
        names(.SD) := lapply(.SD, as.factor),
        .SDcols = c("race", "ethnicity", "suffix")
      ]
      # Also add in the patient protection here
      patients[, 
        race := forcats::fct_lump_prop(race, prop = 0.10)
      ]
      # Here we derive the age by taking today via sysdate and subtracting birthdate to get age in days, divide by 365.241 we get the year
      patients[, 
        age := as.integer((as.IDate(Sys.Date()) - birthdate)) %/% 365.241
      ]
      # Return the table
      patients
    }
  ),
  tar_target(
    
  ),
)
