
# **Healthcare Data Pipeline**
The project aims to build a single source of true data storage for large healthcare datasets using Spark and S3. Some dashboards are also made in this project for visualization.



## **Tech Stack**

- **Data lake:** Amazon S3

- **Data source**: PostgreSQL

- **Data read storage:** MySQL on Amazone RDS

- **Processing layer:** Apache Spark on EMR

- **Visualization:** Power BI


## **Architecture**
The architecture of this project is presented as follows:

![architecture_2](https://github.com/minhky2185/healthcare_data_pipeline/blob/main/images/architecture_2.png)

- Data is sourced from PostgreSQL and ingested into `raw zone` of Data Lake hosted on S3.
- Raw data is cleansed and standardized before moving to `cleansed zone`.
- Cleansed data is transformed into reportable form and loaded into `curated zone`. 
- Publish data from `curated zone` to Data read storage for higher performance report when connection from BI Tool.
- Reports are created in Power BI from the data in MySQL.


## **Data Source**
- Source of raw data is from [CMS](https://data.cms.gov/provider-summary-by-type-of-service). Data used is Medicare Part D.
- Data source in PostgreSQL has 4 tables, total size around 10 GB:
    - Prescriber_drug: ~ 25M rows
    - Prescriber: ~ 1.1M rows
    - Drug: ~115K rows
    - State: ~30K rows
## **Visualization**
Some dashboards create from the data from data read storage
- Drug report

![drug_report](https://github.com/minhky2185/healthcare_data_pipeline/blob/main/images/drug_report.png)

- Prescriber report

![prescriber_report](https://github.com/minhky2185/healthcare_data_pipeline/blob/main/images/prescriber_report.png)

## **Achievement in learning**
### Apache Spark
- Components of Spark and how Spark works.
- How to adjust resource (RAM, CPU, instances,...) for optimizing Spark performance and costs.
- Tuning Spark application by using partition
- Use Spark to implement a full data pipeline.
- Fundamental of how to write Spark correct.
- Manage Jar files for JDBC connection
### Project set up
- Implement logging and log file to track the Spark application
- Test project on local mode before run on cluster.
### AWS
- Set up EMR for Spark
- Track the resource utilization in EMR
