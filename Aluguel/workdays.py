from datetime import timedelta
import datetime as dt
import datetime
import psycopg2

# started from the code of Casey Webster at
# http://groups.google.com/group/comp.lang.python/browse_thread/thread/ddd39a02644540b7

# Define the weekday mnemonics to match the date.weekday function
(MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)
# Define default weekends, but allow this to be overridden at the function level
# in case someone only, for example, only has a 4-day workweek.
default_weekends = (SAT, SUN)


def load_holidays(calendar="BR"):
    try:
        db_conn = psycopg2.connect(
            "host='PGKPTL01' dbname='K11_DB' user='kapitalo11' password='kapitalo11'"
        )
        with db_conn.cursor() as cursor:
            cursor.execute(
                "SELECT dte_Data FROM tbl_feriado where str_calendario='"
                + calendar
                + "'"
            )
            holidays = [item[0] for item in cursor.fetchall()]
            db_conn.close()
            return holidays
    except Exception as e:
        print(str(e))
    return


def networkdays(start_date, end_date, holidays=[], weekends=default_weekends):
    delta_days = (end_date - start_date).days
    full_weeks, extra_days = divmod(delta_days, 7)
    # num_workdays = how many days/week you work * total # of weeks
    num_workdays = (full_weeks + 1) * (7 - len(weekends))
    # subtract out any working days that fall in the 'shortened week'
    for d in range(1, 8 - extra_days):
        if (end_date + timedelta(d)).weekday() not in weekends:
            num_workdays -= 1
    # skip holidays that fall on weekends
    holidays = [x for x in holidays if x.weekday() not in weekends]
    # subtract out any holidays
    for d in holidays:
        if start_date <= d <= end_date:
            num_workdays -= 1
    return num_workdays


def _in_between(a, b, x):

    if isinstance(a, dt.datetime):
        a = a.date()
    if isinstance(b, dt.datetime):
        b = b.date()
    if isinstance(x, dt.datetime):
        x = x.date()

    return a <= x <= b or b <= x <= a


def cmp(a, b):
    return (a > b) - (a < b)


def workday(start_date, days=0, holidays=[], weekends=default_weekends):
    if days == 0:
        return start_date
    if days > 0 and start_date.weekday() in weekends:  #
        while start_date.weekday() in weekends:
            start_date -= timedelta(days=1)
    elif days < 0:
        while start_date.weekday() in weekends:
            start_date += timedelta(days=1)
    full_weeks, extra_days = divmod(days, 7 - len(weekends))
    new_date = start_date + timedelta(weeks=full_weeks)
    for i in range(extra_days):
        new_date += timedelta(days=1)
        while new_date.weekday() in weekends:
            new_date += timedelta(days=1)
    # to account for days=0 case
    while new_date.weekday() in weekends:
        new_date += timedelta(days=1)

    # avoid this if no holidays
    if holidays:
        delta = timedelta(days=1 * cmp(days, 0))
        # skip holidays that fall on weekends
        holidays = [x for x in holidays if x.weekday() not in weekends]
        holidays = [x for x in holidays if x != start_date]
        for d in sorted(holidays, reverse=(days < 0)):
            # if d in between start and current push it out one working day
            if _in_between(start_date, new_date, d):
                new_date += delta
                while new_date.weekday() in weekends:
                    new_date += delta
    return new_date


def isworkday(test_day, holidays=[], weekends=default_weekends):
    return bool(networkdays(test_day, test_day, holidays))


def next_workday(dia=dt.date.today(), calendar="BR"):
    if isinstance(calendar, str):
        calendar = load_holidays(calendar)
    return workday(dia, 1, calendar)


def previous_workday(dia=dt.date.today(), calendar="BR"):
    if isinstance(calendar, str):
        calendar = load_holidays(calendar)
    return workday(dia, -1, calendar)


def num_dus(start_date, end_date, holidays=[], weekends=default_weekends):
    return networkdays(start_date, end_date, holidays) - 1


if __name__ == '__main__':
    print(workday(datetime.date.today(), 3, holidays=load_holidays("BR")))