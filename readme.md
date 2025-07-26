## This is an interview assessment for a data engineer position.
With that said, if you're not part of the interviewing process, kindly look away :)

## File Description:
### main.py:
 - Contains nothing but function calls and the initial data import to dataframe.
Running this file should be enough.
### funcs.py:
 - Contains all functions called by main.py + some supporting functions.
### dataset.csv
 - Well, the source file.


## Function description:
- `line47Table`: filters source file keeping only line 47 related data, breaks it down to two columns (production_start and production_stop containing timestamps) and produces the third column (duration) keeping track of each production wave's length
- `productionFloorStats`: starts by utilizing a support function, `pivotData`, and calculates the production floor's total uptime by using a column created within this function called atLeast1Line, which acts as a flag of whether the whole floor is stopped or if at least one line is up and running. The final calculation is provided by calculating the sum of all timedeltas having atLeast1Line flag be equal to 1.
- `highestDowntime`: again makes use of pivotData and essentially performs a similar task to `productionFloorStats`, only for each production line separately, based on STOP events.
- `pivotData`: this was designed to produce something akin to a matrix, with timestamps used as columns and each of the 4 production lines broken out into their own columns. This matrix is filled with the respective line's and timestamp's status. This process produces many NaN values as lines that started production later or stopped sooner appear in less timestamps. For this, the `fill_Nans` support function is called.
- `fill_Nans`: In this function, any NaN values are filled based on the first or next valid value of each production line. NaN values can appear if the timestamps aren't perfectly aligned between production lines, but by checking the first valid status we can deduce that, for instance, a line has ceased production in a specific moment, so all NaNs above the STOP event can only be ON.

## Setup
If you wish to use this script as a package, install it via pip:
```pip install -i https://test.pypi.org/simple/ productionLineToolbox==0.1.0 ```
Remember the 3 core functions are:
`line47Table(events_df)`
`productionFloorStats(events_df)`
`highestDowntime(events_df)`
and the data is *not hard-coded*. If you wish to test the PyPI package please provide the dataset before running any of the functions.
