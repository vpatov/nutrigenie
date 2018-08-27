
import os
import json
import requests
from bs4 import BeautifulSoup
import urllib3
import time
import csv
import io
from itertools import cycle
import threading
import sys
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
# proxies = {
#   '74.208.217.160','23.89.198.227','199.47.225.57','142.93.202.36'
# }

left_to_download = None
def get_ids(url):
  global headers
  r = requests.get(url,headers=headers,verify=False)
  soup = BeautifulSoup(r.content,'lxml')
  table = soup.find('tbody')
  food_ids = [int(tr.find_all('td')[1].text.strip()) for tr in table.find_all('tr')]
  """
  for tr in table.find_all('tr'):
    food_id = tr.find_all('td')[1].text
  """
  return food_ids


def download_ids():
  url="https://ndb.nal.usda.gov/ndb/search/list?maxsteps=6&format=&count=&max=25&sort=ndb_s&fgcd=&manu=&lfacet=&qlookup=&ds=&qt=&qp=&qa=&qn=&q=&ing=&offset={}&order=desc"
  offset = 0
  food_ids = []
  while(True):
    try:
      batch = get_ids(url.format(offset))
      if len(batch) == 0:
        break
      food_ids += batch
      offset += 25
      if offset % 250 == 0:
        print("Got %d ids." % len(food_ids))
        time.sleep(0.4)
    except Exception as e:
      print(e)
      break


  with open('food_ids.json','w') as f:
    json.dump(food_ids,f)
    print("Wrote %d food_ids to food_ids.json" % len(food_ids))

def download_csvs_job(food_ids,thread_id):
  global headers
  global left_to_download
  # global proxies

  # proxy_pool = cycle(proxies)

  url = "https://ndb.nal.usda.gov/ndb/foods/show/{}?format=Abridged&reportfmt=csv&Qv=1"
  failed_food_ids = []
  start_time = time.time()

  downloaded_ids = set([filename[:filename.index('.')] for filename in os.listdir('csvs')])
  left_to_download = set(food_ids) - downloaded_ids
  while(True):

    if len(food_ids) == 0:
      break

    if len(left_to_download) == 0:
      print("Downloaded all csvs!")
      return True

    count_attempts = 0
    count_successful = 0
    for food_id in left_to_download:


      time.sleep(0.7)

      # proxy = next(proxy_pool)

      r = requests.get(
        url.format(food_id),
        headers=headers,
        verify=False,
        # proxies={"http": proxy, "https": proxy}
      )

      count_attempts += 1

      if count_attempts % 10 == 0:
        time.sleep(1.5)

      if r.status_code != 200:
        failed_food_ids.append(food_id)
        print("{} failed with status code {}.".format(food_id,r.status_code))
        continue

      else:
        count_successful += 1
        elapsed_time = time.time() - start_time
        print(
          "{} SUCCESS: Downloaded {} csvs. Elapsed_time: {:.2f} seconds. "
          "Rate: {:.2f} / sec".format
              (
                food_id,
                count_successful,
                elapsed_time,
                count_successful / elapsed_time
              )
        )



      contents = r.content
      if b'<!DOCTYPE html' in contents:
        failed_food_ids.append(food_id)
        print("%d failed with normal status code :/" % food_id)
        continue

      with open('csvs/{}.csv'.format(food_id),'w') as f:
        f.write(contents.decode('mac_roman'))


    food_ids = failed_food_ids


      


def download_csvs():

  with open('food_ids.json','r') as f:
    food_ids = json.load(f)

  res = download_csvs_job(food_ids,0)
  if res:
    return True

  # num_threads = 1
  # for thread_id in range(0,num_threads):
  #   t = threading.Thread(
  #     target=download_csvs_job,
  #     args=(food_ids[thread_id::num_threads],thread_id))
  #   t.start()


def parse_csv(filename):
  csvfile = open(filename,'r',encoding='mac_roman')
  reader = csv.reader(csvfile)
  lines = [line for line in reader]

  food_name = None
  nutrient_index = None
  unit_index = None
  gram100_index = None
  columns_found = False
  nutrients = {}
  for line in lines:

    if len(line) == 1:
      if 'Nutrient data for:' in line[0]:
        food_name = line[0][line[0].index(':') + 1:].strip()

    if 'Footnotes' in line:
      break

    if not columns_found and len(line) > 1:
      if 'Unit' in line:
        unit_index = line.index('Unit')
      if '1Value per 100 g' in line:
        gram100_index = line.index('1Value per 100 g')
      if 'Nutrient' in line:
        nutrient_index = line.index('Nutrient')

      if (unit_index is not None and gram100_index is not None and 
          nutrient_index is not None):
        columns_found = True

    elif columns_found and len(line) > 1:
      unit = line[unit_index]
      unit_amount = float(line[gram100_index])
      nutrient_name = line[nutrient_index]
      nutrients[nutrient_name] = {
        "unit_amount": unit_amount,
        "unit": unit
        }

  if food_name is None:
    print("food_name is None: %s" % filename)



  food_obj ={
    "name": food_name,
    "nutrients": nutrients
  }

  return food_obj


def parse_csvs():
  foods = []
  for folder in os.listdir('csvs'):
    for filename in os.listdir(os.path.join('csvs',folder)):
      print(os.path.join('csvs',folder,filename))
      food_obj = parse_csv(os.path.join('csvs',folder,filename))
      foods.append(food_obj)

  with open('food_database.json','w') as f:
    json.dump(foods,f)


if 'download' in sys.argv:
  while(True):
    try:
      res = download_csvs()
      if res:
        break
    except Exception as e:
      print(e)
      time.sleep(30)


if 'parse' in sys.argv:
  parse_csvs()





      
        

