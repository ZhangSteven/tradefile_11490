# tradefile_11490
Settlement team downloads a trade file from Bloomberg AIM daily, then they perform 2 tasks:

1. Convert this trade file to China Life trustee format (Excel);
2. Add these trades to an accumulated trade file.

## Caution
1. There are inconsistencies in the date format in the accumulated trade excel file, sometimes it's an excel ordinal, sometimes a string in 'mm/dd/yyyy', in a few cases it's 'dd/mm/yyyy'. Since we just need to convert it once, better run a utility to detect whethere there are sudden date jumps in consecutive trades.
