# -*- encoding: utf-8 -*-
PATH = 'word2vec_model/'
import os
import sys
sys.path.append("/home/goblin/Word2Vec_KL/")
from pymongo import MongoClient
from pyspark.ml.feature import Word2VecModel
from pyspark.sql import SparkSession


import word2vec_model
from dbconfig import DATABASE_NAME
from dbmodel import Connection, Mongo

conn = Connection().getConnection()
mongo = Mongo(conn, DATABASE_NAME)

sys.path.append("..")


def from_db_to_txt(Table_name):
    i = 0
    client = MongoClient('localhost', 27017)
    print(client)
    db = client['News']
    fromdb = db[Table_name]
    field_name = "Текст"
    file_path = "/home/goblin/Word2Vec_KL/TxtFiles/*.txt"

    for rec in fromdb.find({}):
        filename = f'{file_path}_{i}.txt'
        output_file = open(filename, 'w+')
        output_file.write(rec[field_name])
        output_file.close()
        i = i + 1
        if i == 4000:
            break


def main():
    if not os.path.exists('word2vec_model'):
        from_db_to_txt("ArticlesCollection")
        word2vec_model.create_w2v_model()

    spark = SparkSession\
        .builder \
        .appName("Word2VecApplication") \
        .getOrCreate()

    w2v_model = Word2VecModel.load(PATH)



    while True:
        try:
            entry_word = input("Введите слово для поиска синонимов:")
            if entry_word == "-x":
                break
            entry_word = entry_word.replace(' ', '')
            entry_word = entry_word.lower()
            w2v_model.findSynonyms(entry_word, 10).show()
        except Exception as ex:
            print("Данного слова нет в словаре!")
            print(ex)



if __name__ == '__main__':
    main()
