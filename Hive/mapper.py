#!/usr/bin/python
__author__ = 'guichi'

''' for real work '''
import sys
from datetime import datetime
import math
def roundOff(f):
    return math.ceil(f*10)/10

# def truncate(f, n):
#     s = '{}'.format(f)
#     if 'e' in s or 'E' in s:
#         return '{0:.{1}f}'.format(f, n)
#     i, p, d = s.partition('.')
#     return '.'.join([i, (d+'0'*n)[:n]])
def days_between(d1, d2):
    try:
        d1 = datetime.strptime(d1, "%Y-%m-%d %H:%M:%S")
        d2 = datetime.strptime(d2, "%Y-%m-%d %H:%M:%S")
    except(ValueError):
        return 0
    s=(d2-d1).seconds
    f=float(s)/(60*60*24)
    f1=roundOff(f)
    # f1=truncate(f,1)
    d=(d2-d1).days
    return d+float(f1)


last_user=None
last_country=None
last_end_time=None
last_start_time=None

printInfo=None
duration=None
for line in sys.stdin:
    parts=line.strip().split("\t")
    if len(parts)!=3:
        continue

    user=parts[0]
    time_taken=parts[1]
    url=parts[2]
    country=url.split("/")[1]
    # A brand new user
    if last_user != user:
        # exclude the initial one,
        # print the end date of country for previous user
        # map to last or only country for a user
        if last_country and last_end_time:
            # printInfo=printInfo+"\t"+last_end_time

            duration=days_between(last_start_time,last_end_time)

            printInfo=printInfo+"\t"+str(duration)
            print(printInfo)
        # reset
        printInfo=None
        last_start_time=None
        last_end_time=None
        # print("\nNew User : "+user)
        printInfo=user+"\t"+country
        last_start_time=time_taken
    # iterate over the same user
    else:
        if last_country!=country:
            # printInfo=printInfo+"\t"+last_end_time
            duration=days_between(last_start_time,time_taken)
            printInfo=printInfo+"\t"+str(duration)
            print(printInfo)

            printInfo=None
            printInfo=user+"\t"+country
            last_start_time=time_taken
    # update
    last_user=user
    last_country=country
    last_end_time=time_taken
# printInfo=printInfo+"\t"+last_end_time
duration=days_between(last_start_time,last_end_time)
printInfo=printInfo+"\t"+str(duration)
print(printInfo)


