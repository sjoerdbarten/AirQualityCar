# AirQualityCar
GitHub repository related to WUR MAQ Air Quality Car mobile measuring station.

The scripts are divided in two groups:
1) Data collection
2) Post-processing

DATA-COLLECTION:
These are the operational scripts that run during the measurements campaign. They can be run while driving or while being stationary. These scripts retrieve the data from the various communication ports, use the laptop time as reference time, and parse the raw data to .txt files.

POST-PROCESSING:
After a measurement campaign is finished (typically on 1 day), the post-processing scritps are used to post-process the data. The main point here is to homegenize the data using pandas dataframes and later save to .csv files. This data can later be used for analysis. Another point of the post-processing is to allow quick-look graphs of the data. The script can easily be run for any day containing data, and is flexible in which devices have been connected during that drive.
