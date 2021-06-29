import pandas as pd
import html
import re

class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = df[df['retweet_count'] == 'retweet_count' ].index
        df.drop(unwanted_rows , inplace=True)
        df = df[df['polarity'] != 'polarity']

        count = 0
        drop_rows = {}
        for i in df.polarity:
            test = re.findall('[0-9][.0-9]*', str(i))
            if test == []: 
                drop_rows[count] = count
            else: pass
            count += 1

        df.drop(list(drop_rows.values()), axis = 0, inplace = True)
        df.reset_index(drop=True, inplace=True)
        
        return df
    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
        """
        drop duplicate rows
        """
        
        duplicated = df[df.astype(str).duplicated()].index
        df.drop(duplicated, inplace=True)
        
        return df
    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert column to datetime
        """
        
        df['created_at'] = pd.to_datetime(df['created_at'], infer_datetime_format=True)
        
        df = df[df['created_at'] >= '2020-12-31' ]
        
        return df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        df['polarity'] = pd.to_numeric(df['polarity'], downcast='float')
        
        df['subjectivity'] = pd.to_numeric(df['subjectivity'], downcast='float')

        df['favorite_count'] = pd.to_numeric(df['favorite_count'], downcast='float')
        
        df['retweet_count'] = pd.to_numeric(df['retweet_count'], downcast='float')
        
        df['followers_count'] = pd.to_numeric(df['followers_count'], downcast='float')

        df['friends_count'] = pd.to_numeric(df['friends_count'], downcast='float')
        
        return df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        
        df = df[df['lang'] == 'en']
        
        return df

    def clean_text(self, df:pd.DataFrame)->pd.DataFrame:
        """
        cleaning text
        """
        df['extra_clean_text'] = df['cleaned_text'].map(lambda x: re.sub('[:,;\.!?]', '', x))
        df['extra_clean_text'] = df['extra_clean_text'].map(lambda x: re.sub('https?:\/\/.\S+', '', x))
        df['extra_clean_text'] = df['extra_clean_text'].map(lambda x: re.sub('https', '', x))
        df['extra_clean_text'] = df['extra_clean_text'].map(lambda x: re.sub('t co', '', x))
        df['extra_clean_text'] = df['extra_clean_text'].map(lambda x: re.sub('//[A-Za-z]*/[A-Za-z0-9]*', '', x))
        df['extra_clean_text'] = df['extra_clean_text'].map(lambda x: re.sub('@', '', x))
        df['extra_clean_text'] = df['extra_clean_text'].map(lambda x: re.sub('#', '', x))
        df['extra_clean_text'] = df['extra_clean_text'].map(lambda x: x.lower())
        Apos_dict={"'s":" is","n't":" not","'m":" am","'ll":" will",
                   "'d":" would","'ve":" have","'re":" are"}
        for i in Apos_dict.keys():
            df['extra_clean_text'] = df['extra_clean_text'].map(lambda x: re.sub(i, Apos_dict[i], x))

        cols = list(df.columns)
        df = df[cols[0:4] + [cols[-1]] + cols[4:-1]]

        return df
    
    def get_cleaned_tweet_data(self)->pd.DataFrame:
        """
        uses all functions above and returns cleaned dataframe
        """
        
        first_func = self.drop_unwanted_column(self.df)
        second_func = self.drop_duplicate(first_func)
        third_func =self.convert_to_datetime(second_func)
        fourth_func =self.convert_to_numbers(third_func)
        fifth_func =self.remove_non_english_tweets(fourth_func)
        sixth_func =self.clean_text(fifth_func)
        
        print('\n...Automation complete!')
        
        return sixth_func

if __name__ == "__main__":
    file = pd.read_csv('processed_tweet_data.csv')
    # cleaning dataframe
    new_file = Clean_Tweets(file).get_cleaned_tweet_data()
    new_file.to_csv('cleaned_tweet_data.csv', index=False)
    print('FILE SAVED!!!...')