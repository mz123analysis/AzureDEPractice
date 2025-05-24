# AzureDEPractice Summary

Practicing building a full End-to-End Pipeline from Data Source to Dashboard

I want to test Azure and its different services. Time limit for this project will be 30 days and the process.

Data will be man made. This will then be put into an undisclosed SFTP server to mimic getting our files from SFTP. From there I will save files in a local folder.

I will then schedule the creation and importation into our SQL Server hosted on Azure through Apache Airflow and eventually try it with Dagster. 

From there, I will create a simple dashboard with KPIs on PowerBI.

## Step-by-Step Procedure

### File Creation

In the file creation folder, I created a script that would create the initial daily data for our dataset. Using the initial data (Hotel Name and Number of Rooms), new daily data would be created by utilizing Airflow.

With these data files, I will then import them into a SFTP to mimic file transfers to clients AND recieving files.

### Creation of the SFTP Server

Due to SFTP File Transfers being a common way to transfer files among Businesses, I will create a SFTP server in Azure.

On Azure, I first created a Storage Account. The most important part when creating this Storage Account is enabling Hirearchical namespace, which allows you to do SFTP File Transfer. Everything else is up to preference based on what you would want.

Once a Storage Account is created, under settings on the left bar, you will click on SFTP and create a local user. This essentially creates a user and password to allow clients and vendors to access the SFTP server. I created two users to mimic vendor and client.

### Docker Setup --- Airflow

We created another folder that will hold our Airflow setup. We will utilize Docker to help us set this us. Docker provides us some good advantages when it comes to setting up Airflows such as:

 * Fast Deployment
 * Isolation
 * Compatability

Docker Compose allows us to run a multi-container application, with a variety of services we will utilize in this project. It will also allow users to run the same Airflow setup as we have isolated and defined the versions of the software we are using.

### ETL into Database

Pending
