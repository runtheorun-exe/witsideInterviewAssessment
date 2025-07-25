import pandas as pd
import warnings
warnings.filterwarnings("ignore") #I know what this looks like; 
#warning ignored:FutureWarning: ChainedAssignmentError: behaviour will change in pandas 3.0!

def fill_Nans(df):
    df = df.copy()
    for col in df.columns:
        if df[col].notna().any():
            first_valid = df[col].dropna().iloc[0]
            first_valid_label = df[col].first_valid_index()
            first_valid_pos = df.index.get_loc(first_valid_label)
            df[col] = df[col].fillna("ON" if first_valid == "ON" else "STOP")
            df[col].iloc[:first_valid_pos] = (
                "ON" if first_valid == "ON" else "STOP")
    return df

def line47Table(df):
    df = df[df['production_line_id'] == 'gr-np-47']
    line47Start = df.loc[df['status'] == "START", 
                                ['timestamp']].rename(columns = {'timestamp': 'start_timestamp'}).sort_values(by='start_timestamp', ascending=True).reset_index(drop=True)
    line47Stop = df.loc[df['status'] == "STOP", 
                                ['timestamp']].rename(columns = {'timestamp' : 'stop_timestamp'}).sort_values(by='stop_timestamp', ascending=True).reset_index(drop=True)
    line47Events = pd.concat([line47Start, line47Stop], axis=1)
    line47Events['duration'] = pd.to_datetime(line47Events['stop_timestamp']) - pd.to_datetime(line47Events['start_timestamp'])
    print("Line 47 Table:")
    print(line47Events)
    return(line47Events)

def pivotData(df):
    pivot = df.pivot(
    index='timestamp', columns='production_line_id', values='status').reset_index()
    pivot.columns.name = None
    pivot = pivot.sort_index().ffill()
    pivot = fill_Nans(pivot)
    pivot['timedelta'] = pivot['timestamp'].diff()
    return(pivot)

def productionFloorStats(df):
    pivot = pivotData(df)
    onlineStatuses = ['ON', 'START']
    pivot['atLeast1Line'] = pivot.apply(
        lambda row: int(row.isin(onlineStatuses).any()), axis=1)
    # print(pivot.to_string())
    total_uptime = pivot['timedelta'][pivot['atLeast1Line'] == 1].sum()
    print(f"Total Production Floor Uptime: {total_uptime}")
    total_downtime = pivot['timedelta'][pivot['atLeast1Line'] == 0].sum()
    print(f"Total Production Floor Downtime: {total_downtime}")
    return(total_uptime,total_downtime)

def highestDowntime(df):
    downtime_list = []
    df = pivotData(df)
    for col in df.columns:
        if col not in ['timestamp', 'timedelta', 'atLeast1Line']:
            stop_rows = df[df[col] == 'STOP']
            total_downtime = stop_rows['timedelta'].sum()
            downtime_list.append((col, total_downtime))
    downtime_df = pd.DataFrame(downtime_list, columns=['line', 'downtime'])

    max_row = downtime_df.loc[downtime_df['downtime'].idxmax()]

    print(f"Production line with most downtime: {max_row['line']}")
    print(f"Total downtime: {max_row['downtime']}")



events_df = pd.read_csv('dataset.csv', parse_dates=['timestamp'])

#Line 47 Table creation
line47Table(events_df)

# Production Floor Stats (Up/Downtime)
productionFloorStats(events_df)

# Production Line with the most downtime
highestDowntime(events_df)

