SSIS-GDAL
=========

GDAL components for SQL Server Ingetration Services

![alt tag](http://download-codeplex.sec.s-msft.com/Download?ProjectName=GDALSSIS&DownloadId=602441)


Requirements
------------

* Visual Studio 2012
* SQL Server Data Tools
* GDAL >= 1.9


Build
-----

Before building you must create a strong-name key file to sign your assemblies:

	sn.exe -k SSISGDAL.snk
    
Build:

    msbuild.exe build.targets /t:build
    
Install:

    msbuild.exe build.targets /t:install
    
If you have issues check build.targets and make sure the properties are set correctly for your environment  
