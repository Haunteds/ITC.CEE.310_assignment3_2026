# ITC.CEE.310_assignment3_2026
A DataOps assignment  
This repository contains the needed files and configurations to run the job and pipeline in Databricks.  
The execution flow is as follows:  

1. First you will have to copy the repository into your Databricks workspace.
2. Setting up the catalog.
   - For this you can run the Create_catalog.ipynb notebook and it will create the needed catalog, schema and volumes.  
3. Importing the file and dividing it into batches.
   - I haven't implemented the file import from Kaggle automatically so you'll have to import it yourself to the "files" volume.
   - After this you can run the Division_into_batches.ipynb notebook to get the different batches.
4. Creating the ETL pipeline.
   - For this one you can create the pipeline from the Databricks interface "Jobs & Pipelines". Then select ETL pipeline and "Add existing assets".
   - I have provided the root folder and source code in the pipeline folder and also the configurations in "settings_and_configurations" folder.
5. Creating a job
   - This you can do from the same interface as the pipeline creation.
   - The needed files are in the "src" folder and the configurations can be found from the "settings_and_configurations" folder.
   - An easy start is just putting the "reset_batches.py" as the starting task and the "batch_staging.py" and the ETL pipeline will depend on it.
