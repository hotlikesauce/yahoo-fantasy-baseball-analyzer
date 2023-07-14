import datetime

def set_this_week():
    my_date = datetime.date.today()
    year, week_num, day_of_week = my_date.isocalendar()
    
    # Adjust to match All-Star break. 13 for pre-All-Star.
    # The if statement below handles the 2-week ASG Break, which happens on week 30 of the calendar year.
    if week_num < 29:
        return week_num - 13
    else:
        return week_num - 14

def set_last_week():
    my_date = datetime.date.today()
    year, week_num, day_of_week = my_date.isocalendar()
    
    # Adjust to match All-Star break. 13 for pre-All-Star.
    # The if statement below handles the 2-week ASG Break, which happens on week 30 of the calendar year.
    if week_num < 29:
        return week_num - 14
    else:
        return week_num - 15

