from pymongo import MongoClient
from bson import ObjectId
import pymongo
import csv
import re
import datetime

client = MongoClient("mongodb+srv://mongo_db_admin:Qqwerty123456@cluster0.fvfnv.mongodb.net/<dbname>?retryWrites=true&w=majority")


def read_data(csv_file, db, collection):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        tickets_db = client[db]
        tickets_collection = tickets_db[collection]
        for row in reader:
            tickets_collection.insert_one(row).inserted_id

def find_cheapest(db, collection):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    tickets_db = client[db]
    tickets_collection = tickets_db[collection]
    return print(list(tickets_collection.find({}).sort('Цена', pymongo.ASCENDING)))


def find_by_name(name, db, collection):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    tickets_db = client[db]
    tickets_collection = tickets_db[collection]
    regex = re.compile(r'([a-zA-ZА-Яа-яЁё0-9]+)', re.IGNORECASE)
    regex = regex.findall(name)
    regex = '|'.join(regex)
    result = list(tickets_collection.find({'Исполнитель': {'$regex': regex, '$options': 'i'}}).sort('Цена', pymongo.ASCENDING))
    return print(result)

def event_time(db, collection):
    tickets_db = client[db]
    tickets_collection = tickets_db[collection]
    tickets = tickets_collection.find()
    for dict_ticket in tickets:
        date = dict_ticket['Дата']
        date = date.split('.')
        dict_ticket['Дата'] = datetime.datetime(2020, int(date[1]), int(date[0]))
        query = {'_id': ObjectId(dict_ticket['_id'])}
        tickets_collection.replace_one(query, dict_ticket)

def sort_event_by_time(db, collection):
    tickets_db = client[db]
    tickets_collection = tickets_db[collection]
    tickets = list(tickets_collection.find().sort('Дата', pymongo.ASCENDING))
    return print(tickets)

def price_integer(db, collection):
    tickets_db = client[db]
    tickets_collection = tickets_db[collection]
    tickets = tickets_collection.find()
    for dict_ticket in tickets:
        price = dict_ticket['Цена']
        dict_ticket['Цена'] = int(price)
        query = {'_id': ObjectId(dict_ticket['_id'])}
        tickets_collection.replace_one(query, dict_ticket)


if __name__ == '__main__':
    # read_data('artists.csv', 'events_db', 'tickets')
    # tickets_db = client['events_db']
    # tickets_collection = tickets_db['tickets']
    # print(list(tickets_collection.find()))
    # print(list(tickets_collection.find()))
    find_cheapest('events_db', 'tickets')
    # price_integer('events_db', 'tickets')
    # find_by_name('Seconds to mars','events_db', 'tickets')
    # event_time('events_db', 'tickets')
    # sort_event_by_time('events_db', 'tickets')
    # print(tickets_db.list_collection_names())

