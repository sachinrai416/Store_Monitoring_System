from itertools import groupby
import time
import psycopg2
from datetime import datetime, timedelta, timezone
import pytz
import pandas as pd


def get_store_timezone(store_id, timezone_df):
    store_timezone = timezone_df[timezone_df["store_id"] == store_id]
     # If no timezone is defined for the store, assume it is in America/Chicago
    if store_timezone.empty:
        return "America/Chicago"
    return store_timezone

def get_business_hours(store_id, status_df, timezone_df, business_hours_df):
    # Get the timezone for the store
    timezone = get_store_timezone(store_id,timezone_df) 
    # Convert the timestamps in status_df to local time using the store's timezone
    status_df_local = status_df[status_df['store_id'] == store_id].copy()
    status_df_local['timestamp_utc'] = pd.to_datetime(status_df_local['timestamp_utc'])
    status_df_local['timestamp_local'] = status_df_local['timestamp_utc'].dt.tz_localize('UTC').dt.tz_convert(timezone)

    # Get the business hours for the store
    business_hours = {}
    for day in range(7):
        # Check if there is business hours data for the store and day
        mask = (business_hours_df['store_id'] == store_id) & (business_hours_df['day'] == day)
        if any(mask):
            day_data = business_hours_df[mask]
            start_time = datetime.strptime(day_data.iloc[0]['start_time_local'], '%H:%M:%S').time()
            end_time = datetime.strptime(day_data.iloc[0]['end_time_local'], '%H:%M:%S').time()
        # Otherwise, assume the store is open 24*7
        else:
            start_time = datetime.time(datetime(2000, 1, 1, 0, 0))
            end_time = datetime.time(datetime(2000, 1, 1, 23, 59))
        business_hours[day] = {'start_time': start_time, 'end_time': end_time}

    return business_hours



def calculate_uptime_downtime(store_id, store_status_df, business_hours_df, store_timezone_df):
    
    # Get the timezone for the store
    timezone = get_store_timezone(store_id, store_timezone_df)

    #Get a dataframe for unique dates in the store_status_df
    status_dates = store_status_df['timestamp_utc'].dt.date.unique()

    #group the timestamps by date and store it in a list
    timestamps_by_date = {}
    for date, group in groupby(store_status_df['timestamp_utc'], key = lambda x: x.date()):
        timestamps_by_date[date] = list(group)
    #Initialize the uptime and downtime for the store
    uptime = timedelta()
    downtime = timedelta()
    for date in status_dates: 
        day_uptime = timedelta()
        day_downtime = timedelta()
        #Get the business hours for the store
        business_hours = get_business_hours(store_id, store_status_df, store_timezone_df, business_hours_df)
        start_time_local_for_today = business_hours[date.weekday()]['start_time']
        end_time_local_for_today = business_hours[date.weekday()]['end_time']
        print("Business hours start time: " , start_time_local_for_today, "Business hours end time: " , end_time_local_for_today)
        for i in range(len(timestamps_by_date[date]) - 1):
            current_start_time_local = timestamps_by_date[date][i].tz_convert(timezone).time().strftime('%H:%M:%S')
            current_end_time_local = timestamps_by_date[date][i+1].tz_convert(timezone).time().strftime('%H:%M:%S')
            print("Current start time Local: ", current_start_time_local, " Current end time Local: ", current_end_time_local)
            if current_start_time_local < start_time_local_for_today or current_end_time_local > end_time_local_for_today:
                day_downtime += current_end_time_local - current_start_time_local
            else:
                day_uptime += current_end_time_local - current_start_time_local
        
        uptime += day_uptime
        downtime += day_downtime
    return uptime, downtime

def calculate_uptime_downtime_for_stores(store_id, store_status_df, business_hours_df, store_timezone_df):
    # Filter the store_status_df by store_id
    store_status_df = store_status_df[store_status_df["store_id"] == store_id]

    # Convert the timestamp_utc column to a datetime object
    store_status_df["timestamp_utc"] = pd.to_datetime(store_status_df["timestamp_utc"])

    # Get the most recent timestamp for the store
    max_timestamp = store_status_df['timestamp_utc'].max()

    # Calculate uptime and downtime for the last hour
    uptime_last_hour, downtime_last_hour = calculate_uptime_downtime(store_id, store_status_df.loc[store_status_df['timestamp_utc'] >= max_timestamp - timedelta(hours=1)], business_hours_df, store_timezone_df)

    # Calculate uptime and downtime for the last day
    uptime_last_day, downtime_last_day = calculate_uptime_downtime(store_id, store_status_df.loc[store_status_df['timestamp_utc'] >= max_timestamp - timedelta(days=1)], business_hours_df, store_timezone_df)

    # Calculate uptime and downtime for the last week
    uptime_last_week, downtime_last_week = calculate_uptime_downtime(store_id, store_status_df.loc[store_status_df['timestamp_utc'] >= max_timestamp - timedelta(weeks=1)], business_hours_df, store_timezone_df)

    #Convet into minutes
    uptime_last_hour = uptime_last_hour * 60
    downtime_last_hour = downtime_last_hour * 60
    return uptime_last_hour, uptime_last_day  , uptime_last_week  , downtime_last_hour, downtime_last_day  , downtime_last_week 
