# WorkloadTools Power BI Report

To analyze the data produced by WorkloadTools you can use the provided Power BI Template.

In this folder you will find:
* A Sample Power Bi report "WorkloadTools Report - Sample.pbix", you can use it 
* The Power BI template "WorkloadTools Report - Template.pbit", which defines a report structure and will ask for some input parameters before it loading data.

## Overview of the report pages
![Overview](/Images/PowerBI_Overview.png)
![Queries](/Images/PowerBI_Queries.png)
![QueryDetail](/Images/PowerBI_QueryDetail.png)
![WaitStats](/Images/PowerBI_WaitStats.png)


## Usage
1. Download and install Power BI
2. Open the provided template "WorkloadTools Report - Template.pbit" 
3. Provide the connection parameters
4. Explore your data
5. (optional) Save the file for later use, it won't ask again for the parameters

The report allows you to load one or two series of data, if you wanto to visualize only one series leave the optional parameters ("Benchmark *") empty

Required parameters:
* Baseline Server\Instance
* Baseline Database
* Baseline Schema

Optional parameters:
* Benchmark Server\Instance
* Benchmark Database
* Benchmark Schema

![Input Parameters](/Images/PowerBI_InputParams.png)

**Note:**

When using only one serie of data some charts and metrics will be empty, the deltas won't be meaningful.


### Using "Drillthrough" for "Query Detail"

In order to correctly filter the "Query Detail" sheet you need to use the "Drillthrough" function of Power BI.
This function is available on any visual that contains a specific fields (In this case "Sql Hash") and has a properly configured Drillthrough page.

ie: on the "Queries" page you can find several tables with the filed "Sql Hash", right click it and choose "Drillthrough", you will now see the available drillthrough pages, if you click the page, it will show up filtered by the selected "Sql Hash".
To go back to the previous page you can use the arrow in the top-left corner of the page.

![Drillthrough](/Images/PowerBI_InputParams.png)