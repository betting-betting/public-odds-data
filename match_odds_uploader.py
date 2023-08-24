from betfair_data_loader import Betfair_data_loader
from score_event_mapping import score_event_matching
from datetime import datetime,timedelta
from sql import sqlDF,df_to_sql
import time
from config import sports_tables_mapping as mapping
import pandas as pd
from tennis_selection_home_away_matching import tennis_selection_home_away_matching as tennis_selection_matching
from log_notify import Logger,notify
class downloader:
    
    def __init__(self,sport):
        self.sport = sport
        self.log = Logger('logs.log')
        self.notify = notify()

    def update_id_mappings(self,id_data):
        
        id_data['create_ts'] = datetime.now()
        current_betfair_markets = id_data['Betfair_market_id'].to_list()
        
        
        old_mapping_query = """
        select *
        from
        betfair_smarkets_event_market_mapping
        where 1=1
        and date(create_ts) = date(sysdate())"""
        
        
        
        old_mapping = sqlDF(old_mapping_query)
        
        old_betfair_markets = old_mapping['Betfair_market_id'].to_list()
        
        new_markets = [market for market in current_betfair_markets if market not in old_betfair_markets]
        
        upload = id_data.loc[id_data['Betfair_market_id'].isin(new_markets)]
        
        upload['sport'] = self.sport
        
        upload = upload[['event_name','Betfair_event_id','Smarkets_event_id','Betfair_market_id','Smarkets_market_id','create_ts','sport']]
        
        upload = upload.fillna(999)
        
        if len(upload)>0:
            df_to_sql('betfair_smarkets_event_market_mapping',upload)
            print('New mapping added')
            
  
    def update_score_event_mapping(self):
        try:
             matches : pd.DataFrame = score_event_matching(self.sport).matching()
        
             matches['sport'] = 'tennis'
             matches['created_ts'] = datetime.now()
             current_matches = matches['Betfair_event_name'].to_list()
             
             old_matches_query = """
             select *
             from
             score_event_mapping
             where 1=1
             and date(created_ts) = date(sysdate())"""
             
             old_matches = sqlDF(old_matches_query)['Betfair_event_name'].to_list()
             
             new_matches = [match for match in current_matches if match not in old_matches]
             
             upload = matches.loc[matches['Betfair_event_name'].isin(new_matches)]
             
             
             if len(upload)>0:
                 df_to_sql('score_event_mapping',upload)
                 print('New score mapping added')
        except:
            print('No odds data yet')
            
         
    def download(self,refresh,hours,sleep,matched_lower_bound):
        run = 1
        reload = 0
        while True:
            self.log.start()
            try:
                if (datetime.now().minute%refresh == 0 and datetime.now().second in (1,2,3,4,5)) or run == 1 or reload == 1:
                    initiate = Betfair_data_loader(self.sport,datetime.now()-timedelta(hours=4),datetime.now()+timedelta(hours=hours),matched_lower_bound)
                    id_data = initiate.market_event_ids
                    print('Event data reloaded')
                    self.update_id_mappings(id_data)
                    if self.sport == 'tennis' and run != 1 and reload !=1:
                        self.update_score_event_mapping()
                        start = datetime.now()
                        tennis_selection_matching().upload()
                        end = datetime.now()
                        home_away_time = (end-start).total_seconds()
                        if  home_away_time> 60:
                            
                            self.notify.send_message(f'{home_away_time} seconds taken to load home_away data, need to clear tables')
                    time.sleep(0)
                    run = 0
                    reload = 0
            
                else:
                    data = initiate.data_generator()
                    if type(data) != int:
                        df_to_sql(mapping['price_data_tables'][self.sport],data)
                        print('tick')
                        time.sleep(sleep)
                    else:
                        print(f'No events in next {hours} hours with matched volume > {matched_lower_bound}')
            except Exception as e:
                print(e)
                matched_lower_bound+=1000
                print(matched_lower_bound)
                if matched_lower_bound>100000:
                    raise Exception('Api Error')
                    break
                reload = 1
            self.log.stop()
        







