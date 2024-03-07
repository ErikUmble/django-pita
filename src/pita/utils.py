import datetime
from django.utils import timezone

def date_or_datetime(value):
    """
    given either a datetime or a date, this returns the date unless time is specified
    """
    if not isinstance(value, datetime.datetime):
        if value is None or isinstance(value, datetime.date):
            return value
        raise TypeError("value must be a datetime or date object")

    if value.time() == datetime.time.min:
        return value.date()

    return value

def datetime_from_web(date):
    """
    returns a timezone aware datetime or None if invalid
    if date is of isoformat, this simply returns the datetime of it
    if it is of the form yyyy-mm-dd (standard from web form), this gives it time 00:00:00

    """
    if type(date) is datetime.datetime:
        return timezone.make_aware(date)

    if type(date) is datetime.date:
        return timezone.make_aware(
            datetime.datetime.combine(date, datetime.time(0, 0, 0, 0))
        )

    try:
        date = datetime.datetime.fromisoformat(date)
    except (ValueError, TypeError) as e:
        date = None

    if date is not None and not timezone.is_aware(date):
        date = timezone.make_aware(date)

    return date