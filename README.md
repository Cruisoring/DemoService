# DemoService template

This is a template solution for WebAPI Demo service including some powerful utilities to access SQL.


## Start

Since this template is not published yet. Following steps would enable you adding it and create a new solution with this template.

1. Download the repo from [GitHub](https://github.com/Cruisoring/DemoService)
2. From cmd, go into {DemoServiceTemplate}\DemoService.
3. Run `dotnet new -i ./`, you shall see one line displayed
> DemoService        demosvc   [C#]   micro/webapi      
4. Create a new folder like C:\temp\ApiService
5. Run `dotnet new demosvc`, then you shall see new folders and files are created including following in that folder along with other .cs files:
> <DIR> 	ApiService.API
> <DIR>		ApiService.Core
>			ApiService.sln


## Helpers

This template project includes some useful utilities to enhance WebAPI development productivities.

### Secure Settings

The [Settings.cs](./DemoService/DemoService.Core/Settings.cs) introduces a new mechanism to keep the credentials in your home directory that is usually only accessible by yourself.

The [environment_settings.json](./environment_settings.json) in the template folder is an exmaple for you to update and then copied to your home folder that is usually "**C:/users/{your_name}/**". You can add other parameters and update the ConcernedSettings list within [Settings.cs](DemoService.Core/Settings.cs) such as AWS/Azure credentials, ODATA REST API tokens etc.

By setting environment variable **environmentName** to your working environment like **dev**, the dev settings would be loaded automatically.

To switch environment, updating the **environmentName** would enable the Service.Core to get right settings accordingly without changing the source codes.


### SQL Helper

The [SQLHelper.cs](./DemoService/DemoService.Core/Helpers/SQLHelper.cs) provides a powerful set of SQL utilities like:
* Real async executions for each table, row and column that has hardly any sample codes to follow.
* .NET objects to SQLParameters conversions to allow you feed the SQL script with named or ordered arguments without manual preparation.
* Extract SQL script from .sql files for convenience.
* SqlConnection to can be reused: that would save expensive connection setups and tear-downs that have noticeable performance impact especially with high-frequent small queries.
* An disposable SqlTransaction to manage the SqlConnection and commit of multiple executions that are otherwise quite complex to implement.
* With the concept similar to call-backs, a single method is usd to execute all kinds of operations and delegate the post-processing to predefined functions.
* Strong-typed results are returned if model is defined, otherwise dynamic list would be returned.
* Support multi-tables access in multiple ways.

With advanced designs, this SQLHelper has provided more SQL functionalites with least code base than any other published libraries at this time.


### Dynamic Helper

As amendment to the SQLHelper, the [DynamicHelper.cs](./DemoService/DemoService.Core/Helpers/DynamicHelper.cs) provides some handy utilities like:
* Merge tables into list of models with different strategies to handle conflicted values of the same name: this is a quite common scenario with SQL data returned in multiple tables.
* Extract concerned values from a list of dictionaries or dynamic objects.
* Convert single or multiple dyanmic object(s) to Dictionary(ies).
* Compare two dynamic objects or dictionaries to find their differences
* Perform Group-By alike operations upon Dynamic objects or Dictionaries.


## Conclusion

That is all for this time, enjoy.