# **Spotify Data Pipeline using AWS**

## **Overview**
This project extracts, transforms, and loads (ETL) data from **Spotify API** into **AWS services** for analytics using **serverless computing**.

## **Architecture Workflow**
1. **Extract** (Spotify API → AWS Lambda → S3)
   - Fetches data from **Spotify API** using the **Spotipy** package.
   - **AWS Lambda** extracts data and runs on a schedule (daily/weekly) via **Amazon CloudWatch**.
   - Raw data is stored in **Amazon S3**.

2. **Transform** (AWS Lambda → S3)
   - A trigger on **S3** invokes **AWS Lambda** for data transformation.
   - Transformed data is stored back into **S3**.

3. **Load & Analyze** (AWS Glue → Amazon Athena)
   - **AWS Glue Crawler** scans data and infers schema.
   - Schema is stored in **AWS Glue Data Catalog**.
   - **Amazon Athena** enables SQL queries on S3 data.

## **Technologies Used**
- **Spotify API** (Data Source)
- **AWS Lambda** (Serverless Compute)
- **Amazon S3** (Storage)
- **Amazon CloudWatch** (Scheduler)
- **AWS Glue** (Schema Inference)
- **Amazon Athena** (Querying Data)

## **Automation**
- **AWS Lambda & CloudWatch** handle periodic extraction.
- **S3 Triggers** invoke transformation automatically.
- **Glue Crawler** updates schema dynamically.

## **Setup & Deployment**
1. **Obtain Spotify API Credentials** (Client ID & Secret)
2. **Deploy AWS Lambda Functions** for extraction & transformation.
3. **Set Up CloudWatch Triggers** for scheduling.
4. **Enable S3 Triggers** for transformation automation.
5. **Run AWS Glue Crawler** to infer schema.
6. **Query Data in Amazon Athena** using SQL.

## **Usage**
1. Extracts **Spotify Data** → Stores in **S3**.
2. Transforms Data → Saves back in **S3**.
3. Glue Crawler creates **schema metadata**.
4. Athena enables **querying data directly from S3**.

## **Future Enhancements**
- Integrate **Amazon QuickSight** for visualization.
- Implement **data quality checks** in AWS Lambda.

---
This architecture is fully **serverless, automated, and scalable**, leveraging **AWS Cloud** services for seamless data processing. 🚀
