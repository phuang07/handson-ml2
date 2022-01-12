# from datetime import datetime

def time_to_second(time_str):
    # handle time_str in 06:11 format for pace
    if (len(time_str) < 6):
        time_str = '0:' + time_str

    ftr = [3600,60,1]
    return sum([a*b for a,b in zip(ftr, map(int,time_str.split(':')))])

# def second_to_timestring(seconds):

# time_to_second("6:00:14")