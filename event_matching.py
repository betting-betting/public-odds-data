
from smarkets_client import SmarketsClient,percent_to_decimal_dic
from pandas.io.json._normalize import nested_to_record 
from datetime import datetime 

from imp import reload
import config
import betfair_client
reload(betfair_client)
reload(config)
from config import sport_event_mapping as mapping
from betfair_client import betfair_api
from datetime import datetime,timedelta

import pandas as pd
betfair = betfair_api()
smarkets = SmarketsClient()





class event_matching:

    def __init__(self,sport,time_from,time_to):
        self.sport : str = sport
        self.time_from : datetime = time_from
        self.time_to : datetime = time_to
        
    def betfair_event_type_id_mapping(self) -> str:
        return mapping['betfair'][self.sport]
        
    def smarkets_event_type_id_mapping(self) -> str:
        return mapping['smarkets'][self.sport]
        
        
    def betfair_events(self) -> pd.DataFrame:
        event_type_id : str = self.betfair_event_type_id_mapping()
        return betfair.events([event_type_id],self.time_from,self.time_to)
    
    def smarkets_events(self)-> dict:
        
        event_type : str = self.smarkets_event_type_id_mapping()
        
        events = {            
        'team A vs team B':{
            'id':1,
            'name':'name',
            'slug':'slug',            
            }
            }   
                #smarkets.get_available_events(states=['upcoming','live'],
                 #                      types=[event_type],
                 #                      start_datetime_max=datetime.now()+timedelta(hours=20),#self.time_to,
                  #                     start_datetime_min=self.time_from,
                  #                     limit=1,
                  #                     )
        
        return events

    def event_flattening(self,platform,events) -> pd.DataFrame:
        event_manipulation = {}
        
        if platform == 'Betfair':
            splitter : str = ' v '
            
        elif platform == 'Smarkets':
            splitter : str = ' vs '
        
        for i in events:
            value : list = i.split(splitter)
            if len(value)==2:
                event_manipulation[f"{i}"]=value
        
        normalized : list = [x for v in event_manipulation.values() for x in v]
        keys : list = list(event_manipulation.keys())
        
        doubled_keys = []
        for key in keys:
            doubled_keys.append(key)
            doubled_keys.append(key)
        
        flattened = pd.DataFrame(doubled_keys,normalized).reset_index()
        flattened.columns = ['team',f'{platform}_event_name']
        
        if self.sport =='soccer':
            return flattened
        elif self.sport == 'tennis':
            flattened['team'] = [team.split(' ')[-1] for team in flattened['team']]
        return flattened
    
    
    
    
    
        
    def matching(self) -> pd.DataFrame:
        """Matches smarkets and betfair events based on the team names, so be careful with 
        your timeframe because if a team is playing twice in the period it will get messed up"""
        
        betfair_data : pd.DataFrame = self.betfair_events()
       
        smarkets_data : dict = self.smarkets_events()
        
        betfair_events : list = list(betfair_data['event_name'])
        smarkets_events : list = list(smarkets_data.keys())
        
        betfair_flattened :pd.DataFrame = self.event_flattening('Betfair',betfair_events)
        smarkets_flattened :pd.DataFrame = self.event_flattening('Smarkets',smarkets_events)
        
        matches : pd.DataFrame = pd.merge(betfair_flattened,smarkets_flattened,left_on='team',right_on='team',how='left')
        matches = matches.drop_duplicates(subset=['Betfair_event_name'])[['Betfair_event_name','Smarkets_event_name']].reset_index()
        
        return matches,betfair_data,smarkets_data

    
    def ids_matched_events(self) -> pd.DataFrame : 
        """return betfair and smarkets ids for any event of the given event type 
        that is seen on betfair and smarkets within the given timeframe"""
        
        #Fix in here so all betfair events get passed and any missing markets are null
        
        matches,betfair_data,smarkets_data = self.matching()
       
        betfair_events : pd.DataFrame = matches[['Betfair_event_name','index']]
        smarkets_events : pd.DataFrame = matches[['Smarkets_event_name','index']].dropna(subset = ['Smarkets_event_name'])
        
        betfair_matched_data : pd.DataFrame = pd.merge(betfair_data,betfair_events,left_on='event_name',right_on='Betfair_event_name',how='inner')
        
        smarkets_matched_data : pd.DataFrame = pd.DataFrame(smarkets_data)[[i for i in smarkets_events['Smarkets_event_name']]].transpose().reset_index()
        smarkets_matched_data = pd.merge(smarkets_matched_data,smarkets_events,left_on='index',right_on='Smarkets_event_name',how='inner')
        
        matched_data : pd.DataFrame = pd.merge(betfair_matched_data,smarkets_matched_data,left_on='index',right_on='index_y',how='left') 
        
        ids : pd.DataFrame = matched_data[['event_id','id']].reset_index()
        ids.columns=['Pair_id','Betfair_event_id','Smarkets_event_id']
        return ids


class arb_finder(event_matching):
    
    def __init__(self,sport,time_from,time_to,matched_lower_bound):
        #Move matched lower bound to a config and market_type needs to have a mapping from betfair to markets static
        super().__init__(sport,time_from,time_to)
        self.matched_lower_bound : int = matched_lower_bound
        
    
    def market_event_ids(self) -> pd.DataFrame:
        #add in the betfair smarkets event type mapping
        """never filtered on the smarkets matched volume here but assumption is if betfair volume is high then
        smarkets should be too"""
        
        betfair_market_type = 'Match Odds'
        
        if self.sport == 'soccer':
            smarkets_market_type ='Full-time result'
        elif self.sport == 'tennis':
            smarkets_market_type ='Match winner'
        
        event_data : pd.DataFrame = super().ids_matched_events()
        
        betfair_event_ids : list = event_data['Betfair_event_id'].to_list()
        smarkets_event_ids : list = event_data['Smarkets_event_id'].dropna().to_list()
        
        betfair_market_ids : pd.DataFrame = betfair.market_ids(betfair_event_ids,betfair_market_type,self.matched_lower_bound)
        #smarkets_market_ids : pd.DataFrame = smarkets.get_related_markets(smarkets_event_ids,smarkets_market_type)
        
        betfair_merged : pd.DataFrame = pd.merge(event_data,betfair_market_ids,left_on='Betfair_event_id',right_on='Betfair_event_id',how='inner')
        betfair_merged['Smarkets_market_id'] = 'nan'
        #fully_merged : pd.DataFrame = pd.merge(betfair_merged,smarkets_market_ids,left_on='Smarkets_event_id',right_on='Smarkets_event_id',how='left')
        
        
        return  betfair_merged
    
    def betfair_prices(self,betfair_market_ids) -> dict :
        price_data = betfair.price_data(betfair_market_ids)
        data : dict ={}
        for i in price_data:
            market_id : str = i['marketId']
            runners : list = i['runners']
            data[market_id] : dict = {}
            for runner in runners:
                selection_id : int = runner['selectionId']
                book : dict = runner['ex']
                
                best_back_price : float = book['availableToBack'][0]['price']
                best_back_size : float = book['availableToBack'][0]['size']
                
                best_lay_price : float = book['availableToLay'][0]['price']
                best_lay_size : float = book['availableToLay'][0]['size']
                
                data[market_id][selection_id] : dict = {}
                data[market_id][selection_id]['best_back_price'] = best_back_price
                data[market_id][selection_id]['best_back_size'] = best_back_size
                data[market_id][selection_id]['best_lay_price'] = best_lay_price
                data[market_id][selection_id]['best_lay_size'] = best_lay_size
        return data
    
    
    
    
    def smarkets_prices(self,smarkets_market_ids):
        price_data = smarkets.get_quotes(smarkets_market_ids)
        data : dict = {}
        for market_id in price_data.keys():
            data[market_id] : dict = {}
            for selection_id in price_data[market_id].keys():
        
               
                best_back_price : float = smarkets.percent_to_decimal(price_data[market_id][selection_id]['bids'][0]['price'])
                best_back_size : float = price_data[market_id][selection_id]['bids'][0]['quantity']/10000
                
                best_lay_price : float = smarkets.percent_to_decimal(price_data[market_id][selection_id]['offers'][0]['price'])
                best_lay_size : float = price_data[market_id][selection_id]['offers'][0]['quantity']/10000
                
                data[market_id][selection_id] : dict = {}
                data[market_id][selection_id]['best_back_price'] = best_back_price
                data[market_id][selection_id]['best_back_size'] = best_back_size
                data[market_id][selection_id]['best_lay_price'] = best_lay_price
                data[market_id][selection_id]['best_lay_size'] = best_lay_size
        
        return data
    
    def betfair_back_prices_by_event(self,market_event_ids,betfair_price_data):
        #bring size in here maybe
        data = {}
        for i in range(len(market_event_ids)):
            betfair_id = market_event_ids.loc[i,'Betfair_market_id']
            
            A_key = list(betfair_price_data[betfair_id].keys())[0]
            B_key = list(betfair_price_data[betfair_id].keys())[1]
            draw_key = list(betfair_price_data[betfair_id].keys())[2]
            
            a = list(betfair_price_data[betfair_id].values())[0]['best_back_price']
            b = list(betfair_price_data[betfair_id].values())[1]['best_back_price']
            draw = list(betfair_price_data[betfair_id].values())[2]['best_back_price']
            
            data[betfair_id] : dict = {}
            data[betfair_id]['A'] : dict = {}
            data[betfair_id]['B'] : dict = {}
            data[betfair_id]['draw'] : dict = {}
            data[betfair_id]['A']['selection_id'] = A_key
            data[betfair_id]['A']['price'] = a
            data[betfair_id]['B']['selection_id'] = B_key
            data[betfair_id]['B']['price'] = b
            data[betfair_id]['draw']['selection_id'] = draw_key
            data[betfair_id]['draw']['price'] = draw
            
        return data
    
    def smarkets_lay_prices_by_event(self,market_event_ids,smarkets_price_data):
        #bring size in here maybe
        data = {}
        for i in range(len(market_event_ids)):
            smarkets_id = market_event_ids.loc[i,'Smarkets_market_id']
            
            A_key = list(smarkets_price_data[smarkets_id].keys())[0]
            B_key = list(smarkets_price_data[smarkets_id].keys())[2]
            draw_key = list(smarkets_price_data[smarkets_id].keys())[1]
            
            a = list(smarkets_price_data[smarkets_id].values())[0]['best_lay_price']
            b = list(smarkets_price_data[smarkets_id].values())[2]['best_lay_price']
            draw = list(smarkets_price_data[smarkets_id].values())[1]['best_lay_price']
            
            data[smarkets_id] : dict = {}
            data[smarkets_id]['A'] : dict = {}
            data[smarkets_id]['B'] : dict = {}
            data[smarkets_id]['draw'] : dict = {}
            data[smarkets_id]['A']['selection_id'] = A_key
            data[smarkets_id]['A']['price'] = a
            data[smarkets_id]['B']['selection_id'] = B_key
            data[smarkets_id]['B']['price'] = b
            data[smarkets_id]['draw']['selection_id'] = draw_key
            data[smarkets_id]['draw']['price'] = draw
            
        return data
    
    def arb_finder(self):
        market_event_ids : pd.DataFrame = self.market_event_ids()
        
        betfair_market_ids : list = market_event_ids['Betfair_market_id'].to_list()
        betfair_price_data = self.betfair_prices(betfair_market_ids) #inplay flag is in here
        
        smarkets_market_ids : list = market_event_ids['Smarkets_market_id'].to_list()
        smarkets_price_data = self.smarkets_prices(smarkets_market_ids)
        
        betfair_selection_ids : dict = {}
        for market_id in betfair_market_ids:
            betfair_selection_ids[market_id] = list(betfair_price_data[market_id].keys())
        betfair_selection_ids : pd.DataFrame = pd.DataFrame(betfair_selection_ids)
        
        smarkets_selection_ids : dict = {}
        for market_id in smarkets_market_ids:
            smarkets_selection_ids[market_id] = list(smarkets_price_data[market_id].keys())
        smarkets_selection_ids : pd.DataFrame = pd.DataFrame(smarkets_selection_ids)
        
        data_betfair_back = self.betfair_back_prices_by_event(market_event_ids,betfair_price_data)
        data_smarkets_lay = self.smarkets_lay_prices_by_event(market_event_ids,smarkets_price_data) 
        
        for i in range(len(data_betfair_back)):
            betfair_back_A = list(data_betfair_back.values())[i]['A']['price']
            smarkets_lay_A = list(data_smarkets_lay.values())[i]['A']['price']
            
            betfair_back_B = list(data_betfair_back.values())[i]['B']['price']
            smarkets_lay_B = list(data_smarkets_lay.values())[i]['B']['price']
            
            betfair_back_draw = list(data_betfair_back.values())[i]['draw']['price']
            smarkets_lay_draw = list(data_smarkets_lay.values())[i]['draw']['price']
            
            betfair_market_ids[i]
            
            if (smarkets_lay_A<betfair_back_A) :
                print('Success')
                print(smarkets_lay_A)
                print(betfair_back_A)
                print(betfair_market_ids[i])
            else:
                print('Nope')
                print(smarkets_lay_A)
                print(betfair_back_A)
                print(betfair_market_ids[i])
       #smarkets prices look bad, need to check where the bad prices are coming from
       #maybe the percent to decimal is giving bad values?
        return market_event_ids
        
        
                
                
        
        
        

    #Now just need to make function to use the event ids from above and find prices for a market of interest
    #Once prices are got for each pair id, write another function to check if the prices are arbable
    #Then another to execute the trades if it sees the arb
    #Add in some way of logging or seeing trades taken and positions etc

    #For safety should add if a function in each api, "execute at last",
    #if you have an open position for more than 1m then execute at last to close position


