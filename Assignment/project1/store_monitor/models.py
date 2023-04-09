from django.db import models
from django.utils import timezone



class StoreTimezone(models.Model):
    store_id = models.IntegerField(primary_key=True)
    timezone_str = models.CharField(max_length=50, default="America/Chicago")


class BusinessHours(models.Model):
    store_id = models.ForeignKey('Store', on_delete=models.CASCADE)
    day_of_week = models.IntegerField(choices=[
        (0, 'Monday'),
        (1, 'Tuesday'),
        (2, 'Wednesday'),
        (3, 'Thursday'),
        (4, 'Friday'),
        (5, 'Saturday'),
        (6, 'Sunday'),
    ])
    start_time_local = models.TimeField()
    end_time_local = models.TimeField()

    def is_open(self, timestamp=None):
        """
        Returns whether the store is open at the given timestamp.
        If timestamp is not given, the current time is used.
        """
        if not timestamp:
            timestamp = timezone.now()

        # If data is missing for a store, assume it is open 24*7
        if self.start_time_local is None or self.end_time_local is None:
            return True

        day_of_week = timestamp.weekday()
        if day_of_week != self.day_of_week:
            return False

        start_time = timezone.datetime.combine(timestamp.date(), self.start_time_local)
        end_time = timezone.datetime.combine(timestamp.date(), self.end_time_local)

        # If end_time is before start_time, it means the store opens on one day and closes the next day.
        if end_time < start_time:
            end_time += timezone.timedelta(days=1)

        return start_time <= timestamp < end_time

class Store(models.Model):
    store_id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=50)
    address = models.CharField(max_length=100)
    business_hours = models.ManyToManyField(BusinessHours)
    store_timezone = models.CharField(max_length=50, default="America/Chicago")

    def __str__(self):
        return f"{self.name} ({self.store_id})"


class StoreStatus(models.Model):
    store_id = models.IntegerField(primary_key=True)
    status = models.CharField(max_length=50)
    last_updated = models.DateTimeField()


class UptimeDowntime(models.Model):
    store_id = models.ForeignKey(Store, on_delete=models.CASCADE)
    uptime_last_hour = models.IntegerField(default=0)
    uptime_last_day = models.IntegerField(default=0)
    uptime_last_week = models.IntegerField(default=0)
    downtime_last_hour = models.IntegerField(default=0)
    downtime_last_day = models.IntegerField(default=0)
    downtime_last_week = models.IntegerField(default=0)
    timestamp = models.DateTimeField(default=timezone.now)

    @classmethod
    def get_or_create(cls, store_id):
        """
        Returns the UptimeDowntime instance for the given store_id.
        If the instance doesn't exist, creates a new one with default values.
        """
        instance, created = cls.objects.get_or_create(store_id=store_id)
        if created:
            instance.save()
        return instance


class ReportStatus(models.Model):
    report_id = models.CharField(max_length=100, unique=True, primary_key=True)
    status = models.CharField(max_length=20)
    generated_at = models.DateTimeField(auto_now_add=True)
    report_file = models.BinaryField(null=True, blank=True)

    def __str__(self):
        return f"Report ID: {self.report_id} - Status: {self.status}"

