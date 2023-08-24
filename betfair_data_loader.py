from event_matching import arb_finder
from datetime import datetime,timedelta
import pandas as pd
from betfair_client import betfair_api
from smarkets_client import SmarketsClient,percent_to_decimal_dic
import collections
betfair = betfair_api()
smarkets = SmarketsClient()


#arb_finder = arb_finder('soccer',datetime.now(),datetime.now()+timedelta(hours=12),'',0)




class Betfair_data_loader(arb_finder):
    
    def __init__(self,sport,time_from,time_to,matched_lower_bound):
        super().__init__(sport,time_from,time_to,matched_lower_bound)
        self.market_event_ids : pd.DataFrame = super().market_event_ids()
        

    def price_data_sorter(self,data) -> tuple :
        try:
            line1 = data[0]
            price1 = line1['price']
            size1 = line1['size']
        except:
            price1 = -999
            size1 = -999
        try:
            line2 = data[1]
            price2 = line2['price']
            size2 = line2['size']
        except:
            price2 = -999
            size2 = -999
        try:
            line3 = data[2]
            price3 = line3['price']
            size3 = line3['size']
        except:
            price3 = -999
            size3 = -999
        
        return price1,price2,price3,size1,size2,size3

    def data_generator(self) -> pd.DataFrame :
        
        market_event_ids : pd.DataFrame = self.market_event_ids

        betfair_market_ids : list = market_event_ids['Betfair_market_id'].to_list()
        price_data = betfair.price_data(betfair_market_ids)
        
        
        selection_data_columns : list = ['market_id','selection_id','status','matched','last_price',
                      'back_price1','back_price2','back_price3',
                      'back_size1','back_size2','back_size3',
                      'lay_price1','lay_price2','lay_price3',
                      'lay_size1','lay_size2','lay_size3','created_ts']

        final_event_data : list = []
        for data in price_data:
            
                market_id : str = data['marketId']
                event_data : pd.DataFrame = pd.DataFrame(data)
                selection_data : list = []
                for runner in data['runners']:
                    selection_id : int = runner['selectionId']
                    status : str = runner['status']
                    matched : float = runner['totalMatched']
                    try:
                        last : float = runner['lastPriceTraded']
                    except KeyError:
                        last : float = -999
                    ex : dict = runner['ex']
                    timestamp = datetime.now()
                    back_data = ex['availableToBack']
                    lay_data = ex['availableToLay']
                    back_price1,back_price2,back_price3,back_size1,back_size2,back_size3 = self.price_data_sorter(back_data)
                    lay_price1,lay_price2,lay_price3,lay_size1,lay_size2,lay_size3 = self.price_data_sorter(lay_data)
                    selection_data.append([market_id,selection_id,status,matched,last,
                                  back_price1,back_price2,back_price3,
                                  back_size1,back_size2,back_size3,
                                  lay_price1,lay_price2,lay_price3,
                                  lay_size1,lay_size2,lay_size3,timestamp])
                    
                selection_data = pd.DataFrame(selection_data)
                selection_data.columns = selection_data_columns
                event_data : pd.DataFrame = pd.merge(event_data,selection_data,right_on='market_id',left_on='marketId', how='inner')
                event_data = event_data.drop_duplicates(subset=['selection_id'])
                event_data = event_data.drop(['runners','market_id','version'],axis='columns')
                event_data['lastMatchTime'] = pd.to_datetime(event_data['lastMatchTime']) 
                final_event_data.append(event_data)
            
            
        try:
            final_data :pd.DataFrame = pd.concat(final_event_data,ignore_index=True)
            
            final_data.columns=final_event_data[0].columns
        except ValueError:
            return 0
        
        return final_data
        













