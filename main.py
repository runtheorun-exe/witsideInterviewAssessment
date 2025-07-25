import pandas as pd
from funcs import line47Table,productionFloorStats,highestDowntime
import warnings
warnings.filterwarnings("ignore") #I know what this looks like; 
#warning ignored:FutureWarning: ChainedAssignmentError: behaviour will change in pandas 3.0!


events_df = pd.read_csv('dataset.csv', parse_dates=['timestamp'])

#Line 47 Table creation
line47Table(events_df)

# Production Floor Stats (Up/Downtime)
productionFloorStats(events_df)

# Production Line with the most downtime
highestDowntime(events_df)

