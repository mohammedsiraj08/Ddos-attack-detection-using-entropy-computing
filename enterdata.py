from datetime import datetime

from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
collection = client.dataddos.applogs


def enter():
    i=0
    with open('./dataset_requests') as f:
        for line in f:
            try:
                i+=1
                ip_rest = line.split(' - - ')
                ip = ip_rest[0]

                time_rest = ip_rest[1].split(' ')
                time1 = time_rest[0]
                time1 = time1.split('[')[1]
                date1 = time1.split('/')
                day = date1[0]
                month_name = date1[1]
                clock1 = date1[2].split(':')

                year = clock1[0]
                hour = clock1[1]
                minute = clock1[2]
                second = clock1[3]
                time_val = datetime.strptime(
                    year + " " + month_name + " " + day + " " + hour + " " + minute + " " + second,
                    '%Y %b %d %H %M %S')
                timestamp_val = int(datetime.timestamp(time_val) * 10000)

                collection.insert_one({
                    "hostname": ip,
                    "method": "GET",
                    "time": timestamp_val
                })
            except IndexError as e:
                print(e)
                continue


if __name__ == "__main__":
    enter()
