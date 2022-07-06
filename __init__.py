import atexit
from datetime import datetime
from threading import Thread
import pprint
from click import argument
import pymongo
from pymongo import MongoClient

from apscheduler.schedulers.background import BlockingScheduler, BackgroundScheduler

from sliding_window_method import sliding_window
from config import Config

def get_data_in_interval(mongo_resource, start, end):
    try:
        cursor = mongo_resource.find({
            "time": {
                "$gte": start,
                "$lt": end
            }
        }).limit(500)
        return cursor
    except ConnectionError  as e:
        print("Connection Error")
        return []


def generate_time_intervals(start1, interval_size, end1):
    """
   :param  (int) interval_size: interval size to break to in milli seconds
   :param (int) start1: start time in milliseconds
   :param (int) end1: end time in milliseconds
   :return: list of time intervals
   """
    start1 = int(start1)
    end1 = int(end1)
    interval_list1 = []
    n = int((end1 - start1) / interval_size)
    counter = start1
    for i in range(n):
        interval_list1.append([counter, counter + interval_size])
        counter += interval_size

    return interval_list1


def get_result_table(data):
    res = {}
    col_name = "hostname"
    for i in data:
        if i.get('hostname') not in res:
            res[i.get('hostname')] = 1
        else:
            res[i.get('hostname')] += 1
    return res

# start = int(datetime.now().timestamp() * 1000 - float(Config.SLIDING_WINDOW) * 1000)
# end = int(datetime.now().timestamp() * 1000)
#replace this with 63 and 64 line code whenever the data is passed with current timestamp
#else enter the start and end values of time of the requests in the 63 and 64 line

def spawn_threads(mongo_res):
    try:
        start = 8095482340000 #start time of the dataset requests given as input
        end = 8095861050000   #end time of the dataset requests given as input
        interval_list = generate_time_intervals(start, (-start+end)/4, end)
        data_list1 = []
        for lst in interval_list:
            data = get_data_in_interval(mongo_res, lst[0], lst[1])
            data1=get_result_table(data)
            data_list1.append(data1)
        print("Interval list: ", interval_list)
        print("Data list1: ", data_list1)


        slide_thread = Thread(sliding_window(data_list1))
        slide_thread.daemon = True
        # slide_thread.setName("Sliding Window thread")

        slide_thread.start()
        # print("Thread started :" + slide_thread.getName())
    except BaseException as e:
        print("Error in thread")
        print(e)


def task():
    my_client = MongoClient('mongodb://localhost:27017/')
    my_db = my_client['dataddos']
    mongo_res = my_db['applogs']
    interval_size = 20
    beta = Config.BETA
    omega = Config.OMEGA
    spawn_threads(mongo_res)
    


if __name__ == "__main__":
    task()
