from kafka import KafkaConsumer

# Import sys module
import sys
"""
# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name from where the message will recieve
topicName = 'tweet'

# Initialize consumer variable
consumer = KafkaConsumer(topicName, group_id ='group1',bootstrap_servers =
   bootstrap_servers)

# Read and print message from consumer
data_list = []
for msg in consumer:
    data_list.append(msg.value)
    print(msg.value)
    if len(data_list)>=2:
        break
print(data_list)
"""
"""

import os
import re
from operator import add

from pyspark import SparkConf, SparkContext, SQLContext

datafile_json = "../sample-data/description.json"
def get_keyval(row):

    # get the text from the row entry
    text = row

    print(text)

    # for each word, send back a count of 1
    return ["ANDY",1]


def get_counts(df):
    # just for the heck of it, show 2 results without truncating the fields
    df.show (2, False)

    # for each text entry, get it into tokens and assign a count of 1
    # we need to use flat map because we are going from 1 entry to many
    mapped_rdd = df.rdd.flatMap (lambda row: get_keyval (row))

    # for each identical token (i.e. key) add the counts
    # this gets the counts of each word
    counts_rdd = mapped_rdd.reduceByKey (add)

    # get the final output into a list
    word_count = counts_rdd.collect ()

    # print the counts
    for e in word_count:
        print("palabra")
        print (e)



if __name__ == "__main__":
    sc = SparkContext("local","PySpark Word Count Exmaple")
    sqlContext = SQLContext(sc)

    # read the json data file and select only the field labeled as "text"
    # this returns a spark data frame
    df = sc.jsonFile("data.json")
    df.show (2, False)

    # for each text entry, get it into tokens and assign a count of 1
    # we need to use flat map because we are going from 1 entry to many
    mapped_rdd = df.rdd.flatMap (lambda row: get_keyval (row))

    # for each identical token (i.e. key) add the counts
    # this gets the counts of each word
    counts_rdd = mapped_rdd.reduceByKey (add)

    # get the final output into a list
    word_count = counts_rdd.collect ()

    # print the counts
    for e in word_count:
        print (e)
"""
"""
import sys
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
data = spark.read.option("multiline","true").json("data.json")
data.createOrReplaceTempView("country")
country= spark.sql("SELECT location.country FROM country WHERE location is not null")
country.show()
"""
from pyspark import SparkContext
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
from iso639 import languages
from nltk.tokenize import word_tokenize
from wordcloud import WordCloud
plt.style.use('ggplot')
main_lang = "es"
def get_lang(line):
    words = line.split(";;;")
    return [words[1]]

def get_user_location(line):
    words = line.split(";;;")
    return [words[3]]

def get_words(line):
    from nltk.corpus import stopwords
    words = line.split(";;;")
    if words[1] != "es":
        return []
    arr=stopwords.words('spanish')
    arr.extend(["rt","va","11"])
    return [word.lower().replace(".","") for word in words[5].split() if word.lower() not in arr]


def plot_bars(title,xlabel,ylabel,x,y):
    fig = plt.figure()
    fig.set_size_inches(8, 6)
    y_pos = np.arange(len(y))
    plt.bar(y_pos, x, align='center', alpha=0.5)
    plt.xticks(y_pos, y)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.savefig(title+".png",dpi=100)
def plot_word_cloud(title,data, max_num):
    wc = WordCloud(background_color="white",width=1000,height=1000, max_words=max_num).generate_from_frequencies(data)
    wc.to_file(title+'.png')
def get_langs(sc,max_num):
    words = sc.textFile("data.txt").flatMap(get_lang)
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda x: x[1],False)
    result = np.array(wordCounts.collect())
    langs = result[:max_num,0]
    name_langs = []
    for i in langs:
        if i == 'in':
            i='hi'
        if i == 'und':
            name = 'undefined'
        else:
            name = languages.get(alpha2=i).name
        name_langs.append(name)
    quantity = result[:max_num,1].astype(int)
    plot_bars('Idiomas','Idioma','Cantidad de Tweets',quantity,name_langs)

def get_user_locations(sc,max_num):
    words = sc.textFile("data.txt").flatMap(get_user_location)
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda x: x[1],False)
    result = np.array(wordCounts.collect())
    locations = result[:max_num,0]
    quantity = result[:max_num,1].astype(int)
    plot_bars('Paises','Paises','Cantidad de Usuarios',quantity,locations)

def get_words_used(sc,max_num):
    words = sc.textFile("data.txt").flatMap(get_words)
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda x: x[1],False)
    result = np.array(wordCounts.collect())
    dic={}
    for i in range(0,max_num):
        dic[result[i][0]]=int(result[i][1])
    plot_word_cloud('Palabras mas usadas',dic,max_num)

if __name__ == "__main__":

    sc = SparkContext("local","PySpark Word Count Exmaple")
    get_langs(sc,5)
    get_user_locations(sc,5)
    get_words_used(sc,40)