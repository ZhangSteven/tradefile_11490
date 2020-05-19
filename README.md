# tradefile_11490
Settlement team downloads a trade file from Bloomberg AIM daily, then they perform 2 tasks:

1. Convert this trade file to China Life trustee format (Excel);
2. Add these trades to an accumulated trade file.

## Caution
When the conversion program run multiple times on the same 11490 daily trade file (the input), it should modify the accmulated trade file only once.
