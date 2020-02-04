# WorkloadTools Power BI Report

To analyze the data produced by WorkloadTools you can use the provided Power BI Template.

In this folder you will find:
* A Sample Power Bi report "WorkloadTools Report - Sample.pbix", use it play with the report
* The Power BI template "WorkloadTools Report - Template.pbit", which defines a report structure and will ask for some input parameters before it loading data.

## Overview of the report pages
![Overview](/Images/PowerBI_Overview.png)
![WeightOnTotal](/Images/PowerBI_WeightOnTotal.png)
![Queries](/Images/PowerBI_Queries.png)
![QueryDetail](/Images/PowerBI_QueryDetail.png)
![WaitStats](/Images/PowerBI_WaitStats.png)


## Usage
1. Download and install Power BI Desktop
2. Open the provided template "WorkloadTools Report - Template.pbit" 
3. Provide the connection parameters
4. Explore your data
5. (optional) Save the file for later use, it won't ask again for the parameters

The report allows you to load one or two series of data, if you want to visualize only one series leave the optional parameters ("Benchmark") empty

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
This function is available on any visual that contains a specific fields (in this case "Sql Hash") and has a properly configured Drillthrough page.

ie: on the "Queries" page you can find several tables with the field "Sql Hash", right click it and choose "Drillthrough", you will now see the available drillthrough pages, if you click the page, it will show up, filtered by the selected "Sql Hash".
To go back to the previous page you can use the arrow in the top-left corner of the page.

![Drillthrough](/Images/PowerBI_DrillThrough.png)

## Authentication

When connecting to the specified source or sources (if the data are on 2 different server/databases) power Bi will use windows authentication.
If the current user does not have the permission to access the data the load will fail.
To use different credentials you will need to:
* Run the Power BI template
* Save it as a report even if it fails to load the data
* Go to: "File -> options and settings -> data source settings" select the datasource and click "edit permission" now you can set the authentication method and credentials

## Use the Power BI Report on Existing Databases

**What if I already have the data and just want to use the Power BI report?**

Power BI needs a bunch of views in order to load the data, those views are created when some workload data are written to the db for the first time.

You can manually create the views by running the procedure in this folder:
[WorkloadTools\Consumer\Analysis\createAnalysisView.sql](/WorkloadTools/Consumer/Analysis/createAnalysisView.sql)

```
--At least one of the two parameters must be provided, the schema must be the same of your data tables
EXEC [dbo].[createAnalysisView]	@baselineSchema = N'<BaselineSchemaName>', @replaySchema = N'<BenchmarkSchemaName>'
```

## Additional Suggestions

Power BI Desktop does not offer a "wiewer mode", it has been made to create and edit reports. Anyone with the file has full control over it, and can create/edit/delete any visual or measure in the frontend or tables, relationships and M script in the backend.

In order to make the report more usable on Power BI Desktop you can set the following
* **Lock Objects** - this prevents them from moving around while using the report (in the top bar "View" → "Lock Objects" checkbox)
* **Collapse Unused Bars** - this allows you to recover plently of space to view the report, you can collapse the side-bars and the top bar (small arrow i the top right corner)

## Notes For Editors and Curious

If you are new to Power BI and want to make some changes, make your own report or simply know how the report works you may need to know the following

* **Hidden Objects** - to have a more readable report only the strictly necessary is visible in the "Field" panel, some tables, columns and measures (formulas) are hidden. To view them expand the "Field" side-bar, right click and enable "View hidden"
* **Time Field** - For how the Power BI model works:
    * **always** use the field [Elapsed Time (min)] of the "Time" table in any visual that displayes the trend by time (or use the time in general)
    * Any other [Elapsed Time (min)] Field (there is one in almost every table, set as hidden) will not propagate the filter correctly. If used you will obtain flat charts and static numbers

