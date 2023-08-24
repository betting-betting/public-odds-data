import pandas as pd
from sql import sqlDF
import re


class score_event_matching:
    
    def __init__(self,sport):
        self.sport = sport
        
    def read_sql(self,file) -> pd.DataFrame:
        file = open(file,'r')
        query : str = file.read()
        data : pd.DataFrame = sqlDF(query)
        return data
    
    def matching(self):
        score_splits : list = []
        matches : list = []
        odds_data : pd.DataFrame = self.read_sql(r'score_matching_odds_query.txt')
        score_data : pd.DataFrame = self.read_sql(r'score_matching_scores_query.txt')
        
        for i in range(len(score_data)):
            event_name : str = score_data.loc[i,'event_name'].lower()
            split = re.split('-',event_name)
            score_splits.append([event_name,split])
        
        for i in range(len(odds_data)):
            event_name : str = odds_data.loc[i,'event_name']
            event_name_lower = event_name.lower()
            split = re.split(' v | ',event_name_lower)
            split = [i for i in split if len(i)>2]
            for name in split:
                for slug,score_event_name in score_splits:
                    if name in score_event_name:
                        matches.append([event_name,slug])
                        
        matches : pd.DataFrame = pd.DataFrame(matches)
        matches.columns = ['Betfair_event_name','Sofascore_slug']

        matches = matches.drop_duplicates(subset = ['Sofascore_slug'])
        
        events : list = matches['Betfair_event_name'].to_list()
        
        matches['count'] = [events.count(event) for event in events]
        
        matches = matches[matches['count'] == 1][['Betfair_event_name','Sofascore_slug']]
                
        return matches



#so wanna break odds event name into slug like format and then compare that to the slug of the event in the score data
#if n out of x words match then consider it a match and give it a unqiue identifier (maybe surnames and date)
#maybe add this to a new table and then have this script run along with the other two

#then you will have the mapping available in sql

#make this work for soccer too

