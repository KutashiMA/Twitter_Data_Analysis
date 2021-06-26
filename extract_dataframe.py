import json
import pandas as pd
from textblob import TextBlob

def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

def my_zip(value):
    """
    This function returns a list of lists, its an iterator of lists where the first item in each passed iterator is paired together, and
    then the second item in each passed iterator are paired together etc.
    
    This is basically a zip() function but a list instead of an object, which contains lists insted of tuples
    """
    has = {}
    for i in value:
        count = 0
        for j in i:
            if count not in has:
                has[count] = []
                has[count].append(j)
            else:
                has[count].append(j)
            count +=1
    return has.values()

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:
        
        statuses_count = [self.tweets_list[i]['user']['statuses_count'] for i in range(len(self.tweets_list))]
        
        return statuses_count
    
    def find_full_text(self)->list:
        
        lst_one = []
        lst_two = []
        lst_three = []
        text =[]
        count = 0

        for i in self.tweets_list:
            tester = 'retweeted_status' in i.keys()
            if tester==True:
                lst_one.append(count)
            else: pass
            count+=1

        count = 0

        for i in self.tweets_list:
            if  count in lst_one:
                hold = 'extended_tweet' in i['retweeted_status'].keys()
                if hold == True:
                    lst_two.append(count)
            else: pass
            count+=1

        count = 0  

        for i in self.tweets_list:
            if count in lst_two:
                hold = 'full_text' in i['retweeted_status']['extended_tweet'].keys()
                if hold == True:
                    lst_three.append(count)
            else: pass
            count +=1

        count = 0

        for i in self.tweets_list:
            if count in lst_three:
                text.append(i['retweeted_status']['extended_tweet']['full_text'])
            else:
                text.append(None)

            count+=1
        
        return text
    
    
    def find_sentiments(self, text)->list:
        
        polarity = [TextBlob(i).sentiment.polarity for i in text]
        
        subjectivity = [TextBlob(i).sentiment.subjectivity for i in text]
        
        return polarity, subjectivity

    def find_created_time(self)->list:
        
        created_at = [self.tweets_list[i]['created_at'] for i in range(len(self.tweets_list))]
        
        return created_at

    def find_source(self)->list:
        
        source = [self.tweets_list[i]['source'] for i in range(len(self.tweets_list))]

        return source

    def find_screen_name(self)->list:
        
        screen_name = [self.tweets_list[i]['user']['screen_name'] for i in range(len(self.tweets_list))]
        
        return screen_name

    def find_followers_count(self)->list:
        
        followers_count = [self.tweets_list[i]['user']['followers_count'] for i in range(len(self.tweets_list))]
        
        return followers_count

    def find_friends_count(self)->list:
        
        friends_count = [self.tweets_list[i]['user']['friends_count'] for i in range(len(self.tweets_list))]
        
        return friends_count

    def is_sensitive(self)->list:
        
        sensitivity = []
        for i in self.tweets_list:
            if 'possibly_sensitive' in i.keys():
                sensitivity.append(i['possibly_sensitive'])
            else:
                sensitivity.append(None)
            
        return sensitivity

    def find_favourite_count(self)->list:
        
        fav_count = []
        
        for i in self.tweets_list:
            if 'retweeted_status' in i.keys():
                fav_count.append(i['retweeted_status']['favorite_count'])
            else:
                fav_count.append(None)
                
        return fav_count
        
    def find_retweet_count(self)->list:
        
        retweet_count = []
        
        for i in self.tweets_list:
            if 'retweeted_status' in i.keys():
                retweet_count.append(i['retweeted_status']['retweet_count'])
            else:
                retweet_count.append(None)
        
        return retweet_count

    def find_hashtags(self)->list:
        
        hashtags = [self.tweets_list[i]['entities']['hashtags'] for i in range(len(self.tweets_list))]
        
        return hashtags

    def find_mentions(self)->list:
        
        mentions = [self.tweets_list[i]['entities']['user_mentions'] for i in range(len(self.tweets_list))]
        
        return mentions
    
    def find_location(self)->list:
        
        location = [self.tweets_list[i]['user']['location'] for i in range(len(self.tweets_list))]
        
        return location

    def find_place_coord_bound(self)->list:
        lst = []
        try:
            place_coord = [self.tweets_list[i]['place'] for i in range(len(self.tweets_list))]
            for i in place_coord:
                if i is None:
                    lst.append(float("NaN"))
                else:
                    lst.append(i)
            
        except TypeError:
            place_coord = None
        
        return place_coord
    
    def find_lang(self):
        
        lang = [self.tweets_list[i]['lang'] for i in range(len(self.tweets_list))]
        
        return lang

    
        
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        place_coord = self.find_place_coord_bound()
        
        data = my_zip([created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count, screen_name, follower_count, friends_count,\
                       sensitivity, hashtags, mentions, location, place_coord])
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df






                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json("data/covid19.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df(save=True) 

    # use all defined functions to generate a dataframe with the specified columns above

    