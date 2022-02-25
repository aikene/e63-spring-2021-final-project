Selected Topic: Spark for Data Cleaning or ETL

Problem Statement

Using various AWS services, create a serverless ETL process using the Coronavirus data provided by [Indiana Data Hub](https://hub.mph.in.gov/dataset/covid-19-cases-by-school/resource/39239f34-11ff-4dfc-9b9a-a408b0399458?view_id=47850cc1-db3c-435d-8d63-48166ac2c58d). This data should be accessible by AWS Athena, and accessible by a Juypter-like Notebook for further data processing.

Solution Summary

Using the power of Amazon Web Services, I was able to create an ETL process that moved data from an API into an S3 bucket using a lambda function. From there, using the Glue Studio, I was able to author a glue job that moved and transformed that data into parquet format for easier digestion and storage. Using Databricks, I created a cluster where Notebooks could be created for further processing of data within the S3 bucket.

Dataset: [Click Here](https://hub.mph.in.gov/dataset/covid-19-cases-by-school/resource/39239f34-11ff-4dfc-9b9a-a408b0399458?view_id=47850cc1-db3c-435d-8d63-48166ac2c58d)

Technologies Used

-   [AWS Lambda](https://aws.amazon.com/lambda/)
-   [AWS Glue](https://aws.amazon.com/glue/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc)

-   [Glue Studio](https://docs.aws.amazon.com/glue/latest/ug/what-is-glue-studio.html)
-   [Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/populate-data-catalog.html)

-   [AWS Athena](https://aws.amazon.com/athena/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc)
-   AWS Simple Storage Service - [S3](https://aws.amazon.com/s3/)
-   Python

-   AWS SDK for Python [Boto3](https://aws.amazon.com/sdk-for-python/)
-   [URL Lib](https://docs.python.org/3/library/urllib.html)

-   [DataBricks Delta Lake](https://databricks.com/product/delta-lake-on-databricks)

![alt text](![alt text](https://github.com/aikene/e63-spring-2021-final-project/https://github.com/aikene/e63-spring-2021-final-project/blob/master/SparkForETL_Ikene_AJ_Diagram.jpeg?raw=true)
?raw=true)
