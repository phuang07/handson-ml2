from asyncio.runners import run
import sys
import time
import json
import csv
import traceback
from typing import Counter
import requests
import pandas as pd
import asyncio
import aiohttp
import aiofiles
import aiofiles.os
import os
from requests.api import get

# define api endpoints
finisher_filter_api = 'https://results.nyrr.org/api/runners/finishers-filter'
runner_race_api = 'https://results.nyrr.org/api/runners/races'
runner_info_api = 'https://results.nyrr.org/api/runners/detailsRecent'
runner_overview_api = 'https://results.nyrr.org/api/runners/overview'
event_search_api = 'https://results.nyrr.org/api/events/search'

# define csvfiles
race_racers_file = 'data/nyrr/race_racers.csv'
racer_info_file = 'data/nyrr/racer_info.csv'
racer_overview_file = 'data/nyrr/racer_overview.csv'
racer_races_file = 'data/nyrr/racer_races.csv'
events_file = 'data/nyrr/events.csv'

# define global variables
racer_counter=0
event_codes = []


def http_request(url, data):
    headers = {
        'content-type': 'application/json;charset=UTF-8',
        'token': '898d6b6aef0e4887' # Just a ramdon string, but it is necessary
    }
    
    payload = json.dumps(data)
    
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        response.raise_for_status()
        return response.json().get('response')
    
    except Exception as e:
        print(e)
        traceback.print_exc()

# Possible values pass to the api: "{"eventCode":null,"runnerId":null,"searchString":null,"countryCode":"USA","stateProvince":"NY","city":null,"teamName":null,"teamCode":null,"gender":null,"ageFrom":null,"ageTo":null,"overallPlaceFrom":null,"overallPlaceTo":null,"paceFrom":null,"paceTo":null,"overallTimeFrom":null,"overallTimeTo":null,"gunTimeFrom":null,"gunTimeTo":null,"ageGradedTimeFrom":null,"ageGradedTimeTo":null,"ageGradedPlaceFrom":null,"ageGradedPlaceTo":null,"ageGradedPerformanceFrom":null,"ageGradedPerformanceTo":null,"handicap":null,"sortColumn":"overallTime","sortDescending":false,"pageIndex":1,"pageSize":100}"
def get_num_racer(eventCode, countryCode='USA', stateProvince=None,city=None, teamCode=None, gender=None):

    data = {
        'eventCode': eventCode,
        'countryCode': countryCode,
        'stateProvince': stateProvince,
        'city': city,
        'teamCode': teamCode,
        'gender': gender 
    }

    response = http_request(finisher_filter_api, data)

    return response.get('totalItems')
    

def get_race_racers(eventCode, countryCode='USA', stateProvince=None,city=None, teamCode=None, gender=None):
    num_racer = get_num_racer(**locals())
    items_per_page = 1000
    print("num racer:", num_racer)
    page_index = 1
    for i in range(1, num_racer, items_per_page):
        print('page index', page_index)
        data = {
            'eventCode': eventCode,
            'countryCode': countryCode,
            'stateProvince': stateProvince,
            'city': city,
            'teamCode': teamCode,
            'gender': gender,
            'pageSize': items_per_page,
            'pageIndex': page_index
        }

        response = http_request(finisher_filter_api, data)
        # print(response.get('items'))
        write_to_csv(response.get('items'),'race_racers.csv')
        time.sleep(5)
         
        page_index += 1


def fetch_events_by_year_to_file(searchString=None, distance=None, year=None):
    '''
        Using the NYRR search api to get a list of events with details.
        Store in csv file.
    '''
    data = {
        'searchString': searchString,
        'distance': distance,
        'year': year,
        'pageIndex': 1,
        'pageSize': 500
    }
    response = http_request(event_search_api, data) 
    write_to_csv(response.get('items'), events_file)


def write_to_csv(data, output_file):
    data_file = open(output_file, 'a+')
    csv_writer = csv.writer(data_file)
    count = 0
    if (type(data) is dict):
        df = pd.DataFrame.from_dict([data], orient='columns')
        print(df)
        # csv_writer.writerow(data.keys())
        # csv_writer.writerow(data.values())
    elif(type(data) is list):
        for item in data:
            if count == 0:
                # Writing headers of CSV file
                header = item.keys()
                csv_writer.writerow(header)
                count += 1
    
            # Writing data of CSV file
            print(item.values())
            csv_writer.writerow(item.values())
 
    data_file.close()


async def get_racer_info(runner_id):
    global racer_counter

    async with \
        aiohttp.ClientSession() as session, \
        aiofiles.open(racer_info_file, "a+", encoding="utf-8") as f:
        
        headers = {
            'content-type': 'application/json;charset=UTF-8',
            'token': '898d6b6aef0e4887' # Just a ramdon string, but it is necessary
        }
        data = { 'runnerid': runner_id }
        payload = json.dumps(data)

        async with session.post(runner_info_api, data=payload, headers=headers ) as response:
            info = await response.json()
            if (info.get('response') is not None):

                if (racer_counter == 0):
                    file_header = create_file_header_for_racer_info(info.get('response').keys())
                    await f.write(file_header + "\n") 

                values = info.get('response').values()
                converted_list = [str(element) for element in values]
                joined_string = ",".join(converted_list)
                # print(joined_string)
                await f.write(joined_string + "\n")
            else:
                print("Can't get info for runner:", runner_id)
            
            racer_counter += 1
            return racer_counter
        
        await session.close()

def create_file_header_for_racer_race_performance():
    event_codes = get_events_codes()
    columns = ['runnerId']
    for code in event_codes:
        columns.append(code + "_Pace")
        columns.append(code + "_Time")

    return ",".join(columns)

def create_file_header_for_racer_info(keys):
    list(keys).insert(0,'runnerId')
    return ",".join(keys)

async def get_racer_races(runner_id, year):
    '''
    Get all the races a runner has participant in the given year.
    '''
    global racer_counter
    global event_codes

    if (len(event_codes) == 0):
        event_codes = get_events_codes()

    async with \
        aiohttp.ClientSession() as session, \
        aiofiles.open(racer_races_file, "a+", encoding="utf-8") as f:
        
        headers = {
            'content-type': 'application/json;charset=UTF-8',
            'token': '898d6b6aef0e4887' # Just a ramdon string, but it is necessary
        }

        payload = json.dumps({
            "runnerId": runner_id,
            "searchString": None,
            "year": year,
            "distance": None,
            "pageIndex": 1,
            "pageSize": 1000,
            "sortColumn": "EventDate",
            "sortDescending": False
        })

        async with session.post(runner_race_api, data=payload, headers=headers ) as response:
            
            info = await response.json()
            # print(info)
            if (info.get('response') is not None):

                if (racer_counter == 0):
                    file_header = create_file_header_for_racer_race_performance()
                    await f.write(file_header + "\n")
                
                items = info.get('response').get('items')

                race_performance_list = []
                for code in event_codes:
                    actual_pace = None
                    actual_time = None
                    for item in items:
                        if item['eventCode'] == code:
                            actual_pace = item['actualPace']
                            actual_time = item['actualTime']
                    race_performance_list.append(actual_pace)
                    race_performance_list.append(actual_time)


                converted_list = [str(element) for element in race_performance_list]
                joined_string = str(runner_id) + "," + ",".join(converted_list)
                await f.write(joined_string + "\n")
            else:
                print("Can't get races for runner:", runner_id)
            
            racer_counter += 1
            return racer_counter
        
        await session.close()
 


def get_ny_nj_m2019_racers():
    get_race_racers('M2019', stateProvince='NJ')
    get_race_racers('M2019', stateProvince='NY')
    
    # Because the api only allow to get 10,000 records per query
    # and there are more than 10,000 runners from NY state, the following
    # re-run the query for the top 12 cities in NY
    get_race_racers('M2019', stateProvince='NY', city='New York')
    get_race_racers('M2019', stateProvince='NY', city='Brooklyn')
    get_race_racers('M2019', stateProvince='NY', city='Bronx')
    get_race_racers('M2019', stateProvince='NY', city='Astoria')
    get_race_racers('M2019', stateProvince='NY', city='Staten Island')
    get_race_racers('M2019', stateProvince='NY', city='Manhattan')
    get_race_racers('M2019', stateProvince='NY', city='Long Island City')
    get_race_racers('M2019', stateProvince='NY', city='Forest Hills')
    get_race_racers('M2019', stateProvince='NY', city='Flushing')
    get_race_racers('M2019', stateProvince='NY', city='Woodside')
    get_race_racers('M2019', stateProvince='NY', city='Yonkers')

    # remove the duplicate records using dataframe.
    racer_df = pd.read_csv(race_racers_file, index_col='runnerId')
    racer_df.drop_duplicates(inplace=True)
    racer_df.to_csv(race_racers_file)


# get_ny_nj_m2019_racers()

# Async get runner info start here
# This method failed: too many files open.
async def concurrence_fetching2 ():
    runners_df = pd.read_csv(race_racers_file)
    tasks = []

    for index, row in runners_df.iterrows():
        tasks.append(get_racer_info(row['runnerId']))

    await asyncio.gather(*tasks)

def concurrence_fetch_racer_info2 ():
    asyncio.run(concurrence_fetching2())


def post_deal(content):
    '''
    Callback from concurrence_fetch_racer_info(). 
    '''
    print("# racer:", racer_counter)

def concurrence_fetch_racer_info():
    '''    
    See: https://www.programmersought.com/article/34671580884/    
    '''
    runners_df = pd.read_csv(race_racers_file)
    # print(runners_df)
    start = time.time()
    
    loop = asyncio.get_event_loop()
    for index, row in runners_df.iterrows():
        concurrent_request = asyncio.ensure_future(get_racer_info(row['runnerId']))
        concurrent_request.add_done_callback(post_deal)
        loop.run_until_complete(concurrent_request)
    
    end = time.time() - start
    print('time:',end)

def concurrence_fetch_racer_overview():
    '''    
    See: https://www.programmersought.com/article/34671580884/  
    Not used.  
    '''
    runners_df = pd.read_csv(race_racers_file).head(1)
    # print(runners_df)
    start = time.time()
    
    loop = asyncio.get_event_loop()
    for index, row in runners_df.iterrows():
        concurrent_request = asyncio.ensure_future(get_racer_overview(row['runnerId']))
        concurrent_request.add_done_callback(post_deal)
        loop.run_until_complete(concurrent_request)
    
    end = time.time() - start
    print('time:',end)


async def get_racer_overview(runner_id):
    '''
    Get runner's overview race statistics.
    Such as PB for various distance.

    Note:
        Most of the infomation on this method can not be used
        for analysis performance for the past event because it 
        fetch the most up-to-date runner performance stat.

        If analysis based on historical data, then it should create
        another method to fetch the performnce data on the past specific 
        date instead of the current date.

        Implementation for this method is incompleted.
    '''

    global racer_counter

    async with \
        aiohttp.ClientSession() as session, \
        aiofiles.open(racer_overview_file, "a+", encoding="utf-8") as f:
        
        headers = {
            'content-type': 'application/json;charset=UTF-8',
            'token': '898d6b6aef0e4887' # Just a ramdon string, but it is necessary
        }
        data = { 'runnerid': runner_id }
        payload = json.dumps(data)

        async with session.post(runner_overview_api, data=payload, headers=headers ) as response:
            info = await response.json()
            
            if (info.get('response') is not None):

                if(racer_counter == 0):
                    values = info.get('response').keys()
                    converted_list = [str(element) for element in values]
                    joined_string = ",".join(converted_list).replace('bestTimes','')

                    # get best time header
                    bestTimes = info.get('response').get('bestTimes')
                    for bt in bestTimes:
                        keys = bt.keys()
                        converted_list = [str(element) for element in keys]
                        joined_string = joined_string +  ",".join(converted_list)

                    print(joined_string)
                    await f.write(joined_string + "\n")

                # values = info.get('response').values()
                # converted_list = [str(element) for element in values]
                # joined_string = ",".join(converted_list)
                # print(joined_string)
                # await f.write(joined_string + "\n")
            else:
                print("Can't get info for runner:", runner_id)
            
            racer_counter += 1
            return racer_counter
        
        await session.close()


def concurrent_fetch_racer_races_by_year(year):
    '''    
    See: https://www.programmersought.com/article/34671580884/    
    '''
    runners_df = pd.read_csv(race_racers_file)
    # print(runners_df)
    start = time.time()
    
    loop = asyncio.get_event_loop()
    for index, row in runners_df.iterrows():
        concurrent_request = asyncio.ensure_future(get_racer_races(row['runnerId'], year))
        concurrent_request.add_done_callback(post_deal)
        loop.run_until_complete(concurrent_request)
    
    end = time.time() - start
    print('time:',end)


def get_events_codes():
    events_df = pd.read_csv(events_file)
    return events_df.sort_values(by='startDateTime')['eventCode'].to_list()

# def main():
    # get_ny_nj_m2019_racers()
    concurrence_fetch_racer_info()
    # concurrence_fetch_racer_overview()
    # concurrent_fetch_racer_races_by_year(2019)
    # fetch_events_by_year_to_file(2019)

# if __name__ == '__main__':
#     main()
# concurrence_fetch_racer_info()