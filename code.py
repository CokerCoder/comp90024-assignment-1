import json
import re
from collections import defaultdict
from mpi4py import MPI
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
        for line in f:
            try:          
                dictionary = json.loads(line.strip('\n,'))
            except:
                pass  
            else:
                coordinate = dictionary['value']['geometry']['coordinates']
                _id = get_id(coordinate, location_list)
                if _id:
                    text = dictionary['value']['properties']['text']
                    twitter_list.append((_id, text))
                
    return twitter_list

# compute the score of given twitter
def compute_score(text):
    score = 0

    # first check all the phrase (words with two or more words), and if occur, remove it from the text
    for k,v in phrase_dict.items():
        # using regex to find all occurance of the phrase
        phrases = re.findall(r"(?:\s+|^)({}[!,?.'\"]*)(?=\s+|$)".format(k), text, flags = re.I)
        if phrases:
            score += len(phrases) * v
            # remove the corresponding phrase from the twitter
            text = re.sub(r"(?:\s+|^)({}[!,?.'\"]*)(?=\s+|$)".format(k), "", text, flags = re.I)
            
    # clean the unwanted data
    word_list = [word.rstrip('!,?.\'\"').lower() for word in text.split()]

    for word in word_list:
        score += word_dict[word]
    
    return score



if __name__ == '__main__':
    start0 = timeit.default_timer()
    (word_dict, phrase_dict) = read_words('AFINN.txt')
    location_list = load_grids('melbGrid.json')
    
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    recv_data = None
    # root core scatter data to all the cores
    if rank == 0: 
        # load files

        start1 = timeit.default_timer()
        twitter_list = load_twitter('smallTwitter.json')
        stop1 = timeit.default_timer()
        print('Loading Time: ', stop1 - start1)  
        print(f"There are {len(twitter_list)} twitters")

        # allocate tweets evenly
        if len(twitter_list) % size == 0:
            gap = int(len(twitter_list)/size)      
        else:
            gap = int(len(twitter_list)/size) + 1                                                       
        send_data = [twitter_list[x:x+gap] for x in range(0, len(twitter_list), gap)]                                                        
        print("process {} scatter {} data to other processes".format(rank, len(send_data)))
    else:                                                                             
        send_data = None 

    # all cores recevie one of the data and compute seperatly                                                           
    recv_data = comm.scatter(send_data, root=0)
    score_dict = defaultdict(int)
    start2 = timeit.default_timer()
    for _id, text in recv_data:
        score_dict[_id] += compute_score(text)
    stop2 = timeit.default_timer()
    print(score_dict)
    print('Process {} Running Time: {}'.format(rank, stop2 - start2))

    # root core gather all the result dicts and merge them
    send_data = score_dict
    gather_data = comm.gather(send_data, root=0)
    if rank == 0:
        merge_dict = defaultdict(int)
        for sub_dict in gather_data:
            for key, value in sub_dict.items():
                merge_dict[key] += value
        print(merge_dict)
        print('Total Time: ', stop2 - start0) 
      
    