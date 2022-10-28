import pymongo, json
import threading
import os, sys, inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

import resource_manager as ri
import tweet_processing as tp
import cloud_utils as cu

# percorso del file di impostazioni
PATH_SETTING_FILE = "settings.json"

# indica quanti thread sono in esecuzione per il processamento dei tweet
running_threads_preprocessing_tweets = 0


def preprocess_all_shard_tweets(ShardAddress, ShardPort, number_of_threads = 2, wait_for_threads = True):

    # ottenimento impostazioni
    with open(PATH_SETTING_FILE) as json_file:
        setting_data = json.load(json_file)

    # ci si connette a mongos (router)
    mongos_data = setting_data['MongoDB']["Mongos_client"]
    client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])
    db = client_master['TwitterEmotions']

    # ottenimento dati di supporto dal DB NOsql
    slang_dict = find_slang(db)
    emoticon_list = find_emojicon(db)
    stop_word_list = find_stop_words(db)
    neg_word_list = find_negative_words(db)

    # connessione allo shard locale (primario o secondario in base a chi hai paramteri)
    client_shard = pymongo.MongoClient(ShardAddress, ShardPort)
    col = client_shard["TwitterEmotions"].Tweet

    # elenco dei tweet dal DB
    tweet_list = find_tweets(col) #lista di liste [[text,emotion,_id],....]

    # il processamento dei tweet viene effettuato in maniera parallela
    threads = []
    for i in range(number_of_threads):

        # partizionamento dei dati
        start = int((len(tweet_list) * i / number_of_threads) + 1)
        end = int(len(tweet_list) * (i + 1) / number_of_threads)



        if i == 0:
            thread_tweet_list = tweet_list[:end]
        elif i == number_of_threads - 1:
            thread_tweet_list= tweet_list[start:]
        else:
            thread_tweet_list = tweet_list[start:end]

        # creazione del thread
        threads.append(threading.Thread(target=preprocess_tweets_thread,
                                        args=(col, i + 1, thread_tweet_list, emoticon_list,
                                              slang_dict, stop_word_list,neg_word_list)
                                        ))

    # avvio di tutti i thread
    global running_threads_preprocessing_tweets
    running_threads_preprocessing_tweets = number_of_threads

    for t in threads:
        t.start()

    # bisogna aspettare la fine di tutti i thread
    if wait_for_threads:
        for t in threads:
            t.join()


def preprocess_tweets_thread(col, thread_number, tweet_list, emoticon_list, slang_dict, stop_word_list, neg_word_list):

    count = 0

    # le operazioni di bulk rendono più veloce l'inserimento dei dati
    bulk = col.initialize_unordered_bulk_op()

    for tweet in tweet_list:
        # processamento del singolo tweet
        words, emoticons, hashtags = tp.process_tweet(tweet[0], emoticon_list, slang_dict, stop_word_list,neg_word_list)

        # memorizzazione risultati
        bulk.find({'_id': tweet[2]}).update({'$set': {
            "Words": words, "Emoticon": emoticons, "Hashtag": hashtags}})

        count += 1
        if count % 10000 == 0:
            bulk.execute()
            bulk = col.initialize_unordered_bulk_op()
            print("Thread n. ", thread_number, ", processed:", str(count) + "/" + str(len(tweet_list)))

    # esecuzione delle operazioni rimanenti
    if count % 10000 != 0:
        bulk.execute()

    print("Finished to preprocess for thread number:", thread_number)

    # si aggiorna il numero dei thread in esecuzione
    global running_threads_preprocessing_tweets
    running_threads_preprocessing_tweets -= 1
    print("Remaining running threads:", running_threads_preprocessing_tweets)


def find_stop_words(db):
    col = db.StopWord

    stop_word_list = []
    for stop_word in col.find():
        stop_word_list.append(stop_word["Word"])
    return stop_word_list


def find_emojicon(db):
    col = db.Emojicon

    emojicon_dict = {}
    for emoticon in col.find():
        if emoticon["Code"] not in emojicon_dict:
            emojicon_dict[emoticon["Code"]] = emoticon["Polarity"]
    return emojicon_dict


def find_negative_words(db):
    col = db.NegativeWord
    return list(col.find())


def find_slang(db):
    col = db.Slang

    slang_dict = {}
    for slang in col.find():
        if slang["Slang"] not in slang_dict:
            slang_dict[slang["Slang"]] = slang["Traduction"]
    return slang_dict


def find_tweets(col):

    tweet_list = []
    for tweet in col.find({}):
        tweet_list.append([tweet["Text"], tweet["Emotion"], tweet['_id']])

    return tweet_list


def initialise_cluster(setting_data, skip_tweets=False):

    # connessione a mongos
    mongos_data = setting_data["Mongos_client"]
    client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])
    db = client_master["TwitterEmotions"]

    print('Inserting negative words')
    load_negative_word(db)

    print('Inserting slang')
    load_slang(db)

    print('Inserting stop words')
    load_stopwords(db)

    print('Inserting emoji and emoticons')
    load_emojii_emoticon(db)

    if not skip_tweets:
        print('Inserting tweets')
        load_tweet(db)

    print('Inserting word resources')
    load_word_resources(db)

    print("Initialization completed!")


def load_emojii_emoticon(db):

    col = db.Emojicon
    col.delete_many({})
    col.insert_many(ri.get_emojicon())


def load_stopwords(db):
    stopwords_documents = []
    for stop_word in ri.get_stop_words():
        stopwords_documents.append({"Word": stop_word})

    col = db.StopWord
    col.delete_many({})
    col.insert_many(stopwords_documents)


def load_slang(db):
    slang_documents = []
    for key in ri.slang:
        slang_documents.append({"Slang": key, "Traduction": ri.slang[key]})

    col = db.Slang
    col.delete_many({})
    col.insert_many(slang_documents)


def load_negative_word(db):
    negative_word = []
    for line in ri.get_negative_word():
        negative_word.append({"Word": line.strip()})

    col = db.NegativeWord
    col.delete_many({})
    col.insert_many(negative_word)


def load_tweet(db):

    col = db.Tweet
    col.delete_many({})

    tweets = []

    # l'inserimento dei tweets viene effettuato a 'pezzi'
    batch_size = 80000
    count = 0

    for tweet in ri.get_tweets():
        tweets.append({"Text": tweet[0], "Emotion": tweet[1]})
        count += 1

        if count % batch_size == 0:
            col.insert_many(tweets, ordered=False)
            tweets = []

    if count % batch_size != 0:
        col.insert_many(tweets, ordered=False)


def load_word_resources(db):

    col = db.WordCount
    col.delete_many({})

    word_counts = []
    for row in ri.get_resources():

        emotion = row[0]

        word_counts.append({"_id" : {"Emotion" : emotion, 'Word': row[1] }, 'Count': 0, "FlagEmoSN": row[2],
                      "FlagNRC": row[3], "FlagSentisense": row[4]})

    col.insert_many(word_counts)

''' 
CREATING CLOUDS
'''

def create_clouds(setting_data):
    # connessione a mongos
    mongos_data = setting_data["Mongos_client"]
    client_master = pymongo.MongoClient(mongos_data["Address"], mongos_data["Port"])
    db = client_master["TwitterEmotions"]

    # ottenimento conteggi parole, hashtag ed emoticons

    # raggruppamento dei conteggi per parola
    word_count = {}

    for count_row in db.find({}):
        count = count_row['Count']
        if count == 0:
            continue

        word = count_row['_id']['Word']

        if word not in word_count:
            word_count[word] = {}

        word_count[word][count_row['_id']['Emotion']] = count

    hashtag_count = []
    for emotion_count_row in db.HashtagCount.find({}):
        for hashtag_row in emotion_count_row['values']:
            hashtag_count.append({"Hashtag": hashtag_row['hashtag'], "Emotion": emotion_count_row['_id'], "Count": hashtag_row['count']})

    emojicon_count = []
    for emotion_count_row in db.EmojiconCount.find({}):
        for emojicon_row in emotion_count_row['values']:
            emojicon_count.append({"Code": emojicon_row['emoticon'], "Emotion": emotion_count_row['_id'], "Count": emojicon_row['count']})

    cu.make_clouds(word_count, hashtag_count, emojicon_count, True) # passo le frequenze che mi serviranno per creare le wordcloud


