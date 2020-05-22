# tradefile_11490
Read a trade file from Bloomberg (AIM THRP report), do the following:

1. Produce a trade file in China Life trustee format;
2. Produce an accumulated trade file.



## ver 1.01

1. Bug fix: before moving a file to the "processed files" folder, a timestamp is appended to the file name, so that the move won't fail if there is a file with the same name in that folder.



## Ver 1.0
- Put Bloomberg AIM downloaded THRP trade report into a predefined directory. The program scans the directory every 10 minutes for new THRP report. If it finds a new report, the program will run and save the output files into the same directory and move the THRP report into the "processed files" sub folder.

- Output trade file and accumulated trade file are both of type csv. You need to save as Excel if necessary.

- For 11490-B, if a trade is of type "AFS", then its type will be converted to "Trading" in the CL trustee trade file.
