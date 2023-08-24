import pandas as pd
import time

from sql import sqlDF,df_to_sql
from datetime import datetime
from betfair_client import betfair_api

betfair = betfair_api()

class tennis_selection_home_away_matching:
    
    def __init__(self):
        pass
    
    def read_sql(self,path) -> pd.DataFrame:
        file = open(path,'r')
        query : str = file.read()
        data : pd.DataFrame = sqlDF(query)
        return data
        
    def data(self) -> pd.DataFrame:
        data = self.read_sql(r'home_away.txt')
        #data = data.loc[data['player_location'].isnull()]
        return data
    
    def match(self) -> pd.DataFrame:
        data : pd.DataFrame = self.data()
        
        if len(data)>0:
            market_ids : list = list(set(data['marketId']))
            name_data = betfair.selection_id_player_name(market_ids)
            merged : pd.DataFrame = pd.merge(data,name_data,left_on = 'selection_id', right_on = 'selection_id', how = 'inner')\
                [['betfair_event_name','home','away','marketId','selection_id','name']].drop_duplicates(subset = ['selection_id']).reset_index(drop = True)
            
            matched : list = []
            for i in range(len(merged['selection_id'])):
                betfair_event_name : str = merged.loc[i,'betfair_event_name']
                selection_id : int = merged.loc[i,'selection_id']
                market_id : str = merged.loc[i,'marketId']
                name : str = merged.loc[i,'name']
                home : str = merged.loc[i,'home']
                away : str = merged.loc[i,'away']
                print(name.split(' ')[0].lower())
                print(name.split(' ')[-1].lower())
                print(home)
                print(away)
                if name.split(' ')[0].lower() == name.split(' ')[-1].lower():
                    if name.split(' ')[0].lower() in home and name.split(' ')[-1].lower() in home:
                        player_location = 'home'
                        players = home
                    elif name.split(' ')[0].lower() in away and name.split(' ')[-1].lower() in away:
                        player_location = 'away'
                        players = away
                    else:
                        player_location = 'Error'
                        players = 'Error'
                else:
                    if  name.split(' ')[-1].lower() in home:
                        player_location = 'home'
                        players = home
                    elif name.split(' ')[-1].lower() in away:
                        player_location = 'away'
                        players = away
                    else:
                        player_location = 'Error'
                        players = 'Error'
                        
                created_ts = datetime.now()
                matched.append([betfair_event_name,market_id,players,selection_id,player_location,created_ts])
            
            
            matched : pd.DataFrame = pd.DataFrame(matched,columns = ['betfair_event_name','market_id',\
                                                                     'players','selection_id','player_location','created_ts'])
            #removing any events with error
            error_ids : list = list(set(matched.loc[matched['players'] == 'Error','market_id']))
            
            final_matched = matched.loc[~matched['market_id'].isin(error_ids)]
        
            return final_matched
        
        else:
            return 0
                
            
        
    def upload(self):
        matched = self.match()
        if isinstance(matched, pd.DataFrame) and len(matched)>0:
            df_to_sql('tennis_selection_home_away_matching',matched)
            print('Selection matching updated')
            
            