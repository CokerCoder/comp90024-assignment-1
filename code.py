import json

# Read words in to a dictionary
def read_words(filename):
    file = open(filename)
    word_dict = {}
    for line in file.readlines():
        pair = line.strip().split("\t")
        word_dict[pair[0]] = int(pair[1])
    return word_dict


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
        id = get_id(coordinate, location_list)
        if id:
            text = row['value']['properties']['text']
            twitter_list.append((id, text))
    return twitter_list


if __name__ == '__main__':
    word_dict = read_words('AFINN.txt')
    location_list = load_grids('melbGrid.json')
    twitter_list = load_twitter('tinyTwitter.json')

    print(twitter_list[0])