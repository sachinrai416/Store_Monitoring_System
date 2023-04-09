import os
import random
import csv
import threading
from io import StringIO
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
import pandas as pd
import psycopg2
from .models import StoreTimezone, StoreStatus, BusinessHours, ReportStatus
from .utils import calculate_uptime_downtime_for_stores


# connect to the database
conn = psycopg2.connect(
    host="localhost",
    database="Store_Monitoring_Database",
    user="etl",
    password="demopass"
)

cur = conn.cursor()


@csrf_exempt
def trigger_report(request):
    # generate report id
    report_id = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=16))

    # create report status object
    report_status = ReportStatus(report_id=report_id, status='Running')
    report_status.save()

    generate_report(report_id)

    return JsonResponse({'report_id': report_id})



def get_report(request, report_id):
    # get report status object from database
    try:
        report_status = ReportStatus.objects.get(report_id=report_id)
    except ReportStatus.DoesNotExist:
        return JsonResponse({'error': 'Report ID not found'})

    # check if report generation is complete
    if report_status.status == 'Complete':
        # create response
        response = HttpResponse(report_status.report_file, content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="report_{report_id}.csv"'
        return response
    else:
        return JsonResponse({'status': report_status.status})


def generate_report(report_id):
    # get the current directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # define the file paths for the CSV files
    store_status_file = os.path.join(script_dir, '../../Database_Files/store_status.csv')
    business_hours_file = os.path.join(script_dir, '../../Database_Files/business_hours.csv')
    store_timezone_file = os.path.join(script_dir, '../../Database_Files/store_timezone.csv')

    print("Generating report function invoked")
    # get all store ids from StoreStatus table
    store_ids = [timezone.store_id for timezone in StoreTimezone.objects.all()]
    count = len(store_ids)
    # Read the store status data
    store_status_df = pd.read_csv(store_status_file)
    # Read the business_hours.csv file
    business_hours_df = pd.read_csv(business_hours_file)
    # Read the store_timezone.csv file
    store_timezone_df = pd.read_csv(store_timezone_file)

    report_id_csv_columns = ['store_id', 'uptime_last_hour(in minutes)', 'uptime_last_day(in hours)', 'uptime_last_week(in hours)', 'downtime_last_hour(in minutes)', 'downtime_last_day(in hours)', 'downtime_last_week(in hours)']

    filename = f"{report_id}.csv"
    # Create a new CSV file
    with open(filename, 'w', newline='') as file:
        # Create a CSV writer object
        writer = csv.writer(file)

        # Write the column names to the CSV file
        writer.writerow(report_id_csv_columns)
        counter = 0
        for store_id in store_ids:
            # calculate uptime and downtime
            print("Genertaing report for store id:", store_id )
            uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week = calculate_uptime_downtime_for_stores(store_id,store_status_df, business_hours_df, store_timezone_df)

            # Write the data to the CSV file
            print("Uptimes: ", uptime_last_hour, uptime_last_day, uptime_last_week)
            print("Downtimes: ", downtime_last_hour, downtime_last_day, downtime_last_week)
            writer.writerow([store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week])
            counter = counter + 1
            print("Written ",counter," row to CSV file. Remaining lines:" , (count - counter))

    with open(filename, 'rb') as file:
        report_file_contents = file.read()

    # update report status object in database
    report_status = ReportStatus.objects.get(report_id=report_id)
    report_status.status = 'Complete'
    report_status.generated_at = timezone.now()
    report_status.report_file = report_file_contents
    report_status.save()
    print("Generating report done.")
    
