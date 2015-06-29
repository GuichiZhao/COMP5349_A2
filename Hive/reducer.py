#!/usr/bin/python
__author__ = 'guichi'
import sys
import math

def roundOff(f):
    return math.ceil(f*10)/10

user_map={str:[]}
last_user=None
printInfo=None
for line in sys.stdin:

    parts=line.strip().split("\t")
    user=parts[0]
    country=parts[1]
    duration=parts[2]
    if last_user!=user:
        if user_map.values()!=[[]]:
            printInfo=last_user+"\t"
            for key in user_map.keys():
                countryList=user_map[key]
                countryList.sort()
                l=countryList.__len__()
                max=countryList[l-1]
                min=countryList[0]
                total=0.0
                for c in countryList:
                    total+=float(c)
                avg=total/l
                i=key+"("+str(l)+","+str(max)+","+str(min)+","+str(roundOff(avg))+","+str(total)+"),"
                printInfo+=i
            printInfo=printInfo[0:-1]
            print(printInfo)

        # print(user)
        user_map.clear()
        lst=[]
        lst.append(duration)
        user_map[country]=lst
    else:
        if not user_map.has_key(country):
            user_map[country]=[]
        lst=user_map[country]
        lst.append(duration)
    last_user=user
printInfo=last_user+"\t"



for key in user_map.keys():
    # print("Key is : "+key)
    countryList=user_map[key]
    countryList.sort()
    l=countryList.__len__()
    max=countryList[l-1]
    min=countryList[0]
    total=0.0
    for c in countryList:
        total+=float(c)
    avg=total/l
    i=key+"("+str(l)+","+str(max)+","+str(min)+","+str(roundOff(avg))+","+str(total)+"),"
    printInfo+=i
printInfo=printInfo[0:-1]
print(printInfo)




