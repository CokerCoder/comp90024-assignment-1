import json
import re
from collections import defaultdict

import timeit

# Read words in to a dictionary
def read_words(filename):
    file = open(filename)
    word_dict = {}
    for line in file.readlines():
        pair = line.strip().split("\t")
        word_dict[pair[0]] = int(pair[1])
    
    # filter out the phrases with two or more words
    phrase_dict = defaultdict(int, {k: v for k, v in word_dict.items() if len(k.split())>1})

    # keep the words has only one word
    word_dict = defaultdict(int, {k : word_dict[k] for k in set(word_dict) - set(phrase_dict)})

    return (word_dict, phrase_dict)


# Load melbGrid
def load_grids(filename):
    location_list = [] # Store grid information
    with open(filename, 'r') as f:
        grid_dic = json.load(f)
        for location_dict in grid_dic['features']:
            location_list.append(location_dict['properties'])
    return location_list


# Given coordinate and grid information return a location id
def get_id(coordinate, location_list):
    x = coordinate[0]
    y = coordinate[1]
    for location in location_list:
        if x > location['xmin'] and x <= location['xmax'] \
            and y > location['ymin'] and y <= location['ymax']:
            return location['id']
    return False


# Load coordinate and text
def load_twitter(filename):
    twitter_list = [] # Only store tweets in the specified location
    with open(filename, 'r', encoding = 'utf-8') as f:
        twitter_dic = json.load(f)

    for row in twitter_dic['rows']:
        coordinate = row['value']['geometry']['coordinates']
        _id = get_id(coordinate, location_list)
        if _id:
            text = row['value']['properties']['text']
            twitter_list.append((_id, text))
    return twitter_list


# compute the score of given twitter
def compute_score(word_dict, phrase_dict, text):
    score = 0

    # first check all the phrase (words with two or more words), and if occur, remove it from the text
    for k,v in phrase_dict.items():
        # using regex to find all occurance of the phrase
        phrases = re.findall(r"(?:\s+|^)({}[!,?.'\"]*)(?=\s+|$)".format(k), text, re.IGNORECASE)
        if phrases:
            score += len(phrases) * v
            # remove the corresponding phrase from the twitter
            text = re.sub(r"(?:\s+|^)({}[!,?.'\"]*)(?=\s+|$)".format(k), "", text)
            
    # clean the unwanted data
    word_list = [word.rstrip('!,?.\'\"').lower() for word in text.split()]

    for word in word_list:
        score += word_dict[word]
    
    return score



if __name__ == '__main__':
    start = timeit.default_timer()

    (word_dict, phrase_dict) = read_words('AFINN.txt')
    location_list = load_grids('melbGrid.json')
    twitter_list = load_twitter('smallTwitter.json')

    print(f"There are {len(twitter_list)} twitters")

    score_dict = defaultdict(int)

    for _id, text in twitter_list:
        score_dict[_id] += compute_score(word_dict, phrase_dict, text)

    print(score_dict)

    stop = timeit.default_timer()
    print('Time: ', stop - start)  