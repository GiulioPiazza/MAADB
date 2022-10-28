import re
import emoji
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag
from nltk.corpus import wordnet
from nltk.tokenize import TweetTokenizer
import resource_manager as ri

tweet_tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True, reduce_len=True)
lemmatizer = WordNetLemmatizer()
all_punctuations = ri.get_punctuations()


def remove_nick_and_url(line):
    if "USERNAME" in line:
        line = line.replace("USERNAME", " ")
    if "URL" in line:
        line = line.replace("URL", " ")
    return line


def substitute_slangs(line, slang_dict):
    for slang in slang_dict:
        slang = " " + slang + " "
        standard = " " + slang_dict[slang.replace(" ", "")] + " "

        if line.startswith(slang[1::]):
            line = line.replace(slang[1::], standard) + line[len(slang[1::])::]
        if slang in line:
            line = line.replace(slang, standard)
        if line.endswith(slang[::-1]):
            line = line[:-len(slang[1::])] + line.replace(slang[::-1], standard)
    return line


def substitute_contractions(line):
    for contraction in ri.contractions:
        line = line.replace(" " + contraction + " ", " " + ri.contractions[contraction] + " ")

    # effettuo una seconda passata
    for contraction in ri.contractions:
        line = line.replace(contraction, ri.contractions[contraction])

    return line

def find_hashtags(line):

    hashtags = []

    for hashtag in re.findall(r"#(\w+)", line): #cerchiamo tutti gli hastag nel tweet
        hashtags.append(hashtag)
        line = line.replace("#" + hashtag, "")

    return line, hashtags


def find_standard_emojis(line):

    new_line = ""
    emojis = []

    for c in line:
        if c in emoji.UNICODE_EMOJI:
            emojis.append(c)
        else:
            new_line += c

    return line, emojis


def find_emojicons(line, emoticon_list):

    emojicons = []
    for emoticon in emoticon_list:
        while line.find(emoticon) != -1:
            line = line.replace(emoticon, "", 1)
            emojicons.append(emoticon)

    return line, emojicons


def process_tweet(line, emojicon_list, slang_dict, stop_word_list, neg_word_list):

    # rimozione nickname e url
    line = remove_nick_and_url(line)

    # find hashtag
    line, line_hashtag_list = find_hashtags(line)

    # find emoticons ed emoji in tweet that match an emojicon in the input dict
    line, line_emojicon_list = find_emojicons(line, emojicon_list)
    line, standard_emoji_list = find_standard_emojis(line)

    line_emojicon_list.extend(standard_emoji_list)
    del standard_emoji_list

    # i caratteri vengono normalizzati
    line = line.lower()

    # sostituzione delle contrazioni
    line = substitute_contractions(line)

    # sostituzione dello slang
    line = substitute_slangs(line, slang_dict)

    # tokenizzazione e lemmatizzazione
    tokens = tweet_tokenizer.tokenize(line)
    lemmas = lemmatize_tokens(tokens)

    result_lemmas = []

    '''
    # si memorizzano i lemmi che non rientrano tra le stop word o tra i caratteri di punteggiatura
    # si implementa una semplice considerazione delle parole negative (quelle che precedono subito prima una parola)
    '''
    next_word_negated = False
    for lemma in lemmas:
        if lemma in neg_word_list:  #se il lemma è not per esempio si mette a true la variabile cosi da salvare la prossima parola con not + lemma (not cute)
            
            next_word_negated = not next_word_negated  # la prossima parola sarà una parola negativa
        else:
            if lemma not in stop_word_list and lemma not in all_punctuations and not lemma.isdigit():
                # lemma normale, si tratta anche la negazione semplice
                if next_word_negated:
                    result_lemmas.append("not " + lemma)  
                else:
                    result_lemmas.append(lemma)

            next_word_negated = False
    return result_lemmas, line_emojicon_list, line_hashtag_list


# dizionario che mappa il corretto PoS per il lemmatizer seguente
tag_dict = {"J": wordnet.ADJ,
            "N": wordnet.NOUN,
            "V": wordnet.VERB,
            "R": wordnet.ADV}


def lemmatize_tokens(tokens):
    """Implementa la lemmatizzazione dei tokens, considerando il PoS"""
    result = []

    # determinazione PoS
    pos_tags = pos_tag(tokens)

    for i in range(len(pos_tags)):
        #ottenimento tag
        tag = pos_tags[i]
        wn_pos_tag = tag_dict.get(tag[1][0].upper(), wordnet.NOUN)  #wordnet.NOUN if not found (): default value

        # lemmatizzazione
        lemma = lemmatizer.lemmatize(tokens[i], wn_pos_tag)
        result.append(lemma)

    return result
