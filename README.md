# tradefile_11490
Read a trade file from Bloomberg (AIM THRP report), then perform 2 tasks:

1. Convert this trade file to China Life trustee format;
2. Create an accumulated trade file that include all trades since year 2015, including the trades in this trade file.


## To do
1. For 11490-B, if the type of trade is "AFS", convert to "Trading" in the output CL trustee trade file.


## Ver 1.0
- Output trade file and accumulated trade file are both of type csv.

- There is no database involved, the accumulated trade file is built by adding today's trades to the nearest accumulated trade file in the output directory.

