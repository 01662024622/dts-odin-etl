from datetime import date, datetime, timedelta
import calendar


def get_weekend_timestamp(year_week_id):
    date_time = datetime.strptime(str(year_week_id), "%Y%W")
    print('date_in_week: ', date_time.weekday())
    print('date_time_a: ', date_time)
    weekend_timedelta = (6 - date_time.weekday())
    weekend = date_time + timedelta(weekend_timedelta)
    weekend_timestamp = (calendar.timegm(weekend.utctimetuple()))
    year_week_id = (date_time.strftime("%s%f"))
    print('date_time: ', weekend)
    print('year_week_id: ', weekend_timestamp)
    return weekend_timestamp


if __name__ == '__main__':
    print('hello')
    a = get_weekend_timestamp(202018)
    print('a: ', a)

