# WorkloadTools Power BI Report

To analyze the data produced by WorkloadTools you can use the provided Power BI Template.

In this folder you will find:
* A Sample Power Bi report "WorkloadTools Report - Sample.pbix", you can use it 
* The Power BI template "WorkloadTools Report - Template.pbit", which defines a report structure and will ask for some input parameters before it loading data.

## Overview of the report pages
![Input Parameters](./Images/PowerBI_Overview.png)
![Input Parameters](./Images/PowerBI_Queries.png)
![Input Parameters](./Images/PowerBI_QueryDetail.png)
![Input Parameters](./Images/PowerBI_WaitStats.png)


## Usage
1. Download and install Power BI
2. Open the provided template "WorkloadTools Report - Template.pbit" 
3. Provide the connection parameters
4. Explore your data
5. (optional) Save the file for later use, it won't ask again for the parameters

The report allows you to load one or two series of data, if you wanto to visualize only one series leave the optional parameters ("Benchmark *") empty

The required parameters are:
* Baseline Server\Instance
* Baseline Database
* Baseline Schema
The optional parameters are:
* Benchmark Server\Instance
* Benchmark Database
* Benchmark Schema

**Note:**
When using only one serie of data some charts and metrics will be empty, the deltas won't be meaningful

This is the 
![Input Parameters](./Images/PowerBI_InputParams.png)
