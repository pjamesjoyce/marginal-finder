import pandas as pd
import json
from time import sleep
from collections import Counter
import urllib
import os

this_path = os.path.dirname(os.path.realpath(__file__))
assets = os.path.join(this_path, "assets")

class ComtradeMarketFinder:
    def __init__(self, 
                 prod_fname = None,
                 codes_fname = None,
                 year=2003,
                 tMarket = 0.05,
                 market = "Belgium",
                 commodity_codes = [252329, 252321],
                 tYear = 0.5,
                 years = [x for x in range(2003,2014)]
                ):
        
        self._get_production_data(prod_fname)
        self._get_codes(codes_fname)
        self.year = year
        self.tMarket = tMarket
        self.market = market
        self.commodity_codes = commodity_codes
        self.tYear = tYear
        self.years = years
            
    def _get_production_data(self, fname = None):
        
        if fname is None:
            fname = os.path.join(assets, 'Cement_Production_data.csv')
            
        self.production_data = pd.read_csv(fname)
        
    def _get_production_amount(self, code, year = None):
        
        if year is None:
            year = self.year
            
        if int(code) in self.production_data['Comtrade_No'].tolist():
        
            ix_market = self.production_data.index[self.production_data['Comtrade_No'] == int(code)].tolist()[0]
            
            production_amount = self.production_data.at[ix_market, str(year)]
        
        else:
            print ('No Production data for {}. Setting production amount to zero'.format(self._get_country_name(code)))
            production_amount = 0
                
        return production_amount
    
    def _get_codes(self, fname = None):
        
        if fname is None:
            fname = os.path.join(assets, 'partnerAreas.json')
        with open(fname, 'r') as f:
            comtrade_data = json.load(f)['results']
            self.comtrade_code_dict = {x['text']:x['id'] for x in comtrade_data}
            
    def _get_country_code(self, name):
        
        return self.comtrade_code_dict[name]
    
    def _get_country_name(self, code):
        
        rev_dict = {v:k for k, v in self.comtrade_code_dict.items()}
        
        return rev_dict[code]
    
    def _get_comtrade_import_data_url(self, importer_code = None, year = None):
        
        if importer_code is None:
            importer_code = self._get_country_code(self.market)
            
        if year is None:
            year = self.year
            
        
        api_data = {
            '_max' : "50000",
            '_type' : "C",
            'freq' : "A",
            'px' : "HS",
            'p' : "all",
            'ps' : year,
            'rg' : 1,
            'fmt' : "CSV",
            'r' : importer_code,
            'cc' : "%2C".join(str(c) for c in self.commodity_codes)
        }
        
        url = "http://comtrade.un.org/api/get?max={_max}&type={_type}&freq={freq}&px={px}&ps={ps}&r={r}&p={p}&rg={rg}&cc={cc}&fmt={fmt}".format(**api_data)
        
        return url
    
    def _parse_comtrade_data(self, importer_code=None, market_production = None, year = None, tMarket=None):
        
        if importer_code is None:
            importer_code = self._get_country_code(self.market)
            
        if year is None:
            year = self.year
        
        if tMarket is None:
            tMarket = self.tMarket
            
        if market_production is None:
            market_production = self._get_production_amount(importer_code, year)
        
        url = self._get_comtrade_import_data_url(importer_code, year)
        
        
        timeouts = [5,10,20,20]
        #str_error = None
        
        for i in range(4):
            try:
                comtrade_df = pd.read_csv(url)
                break

            except (urllib.error.HTTPError, ConnectionResetError) as err:
                
                print(err)
                #print(err.code, err.reason, err.headers)
                
                timeout = timeouts[i]
                print('Waiting {} seconds and trying again...'.format(timeout))
                sleep(timeout)
                print('Trying again')
        
        useful_columns = ['Year', 'Trade Flow', 'Reporter Code',
           'Reporter', 'Reporter ISO', 'Partner Code', 'Partner', 'Partner ISO',
           'Commodity Code', 'Commodity', 'Netweight (kg)', 'Trade Value (US$)']
        
        not_world = comtrade_df['Partner ISO'] != 'WLD'
        
        parsed_comtrade_df = comtrade_df[useful_columns][not_world]
        
        parsed_import_df = pd.DataFrame(parsed_comtrade_df.groupby(['Reporter ISO', 'Reporter','Reporter Code', 'Partner ISO', 'Partner', 'Partner Code']).aggregate(sum)['Netweight (kg)'])
        
        parsed_import_df['Share'] = parsed_import_df['Netweight (kg)']/market_production
        
        keep = parsed_import_df['Share'] > tMarket
        
        return parsed_import_df, parsed_import_df[keep]
    
    def find_markets(self, year=None, tMarket=None):
        
        if year is None:
            year = self.year
        
        if tMarket is None:
            tMarket = self.tMarket
        
        stop_n = 50
        
        initial_code = self._get_country_code(self.market)
        found_markets = [initial_code]
        
        print("="*100)
        print(year)
        print("-"*100)
        print('Iteration 1... Checking {}'.format(self._get_country_name(initial_code)))
        print("-"*100)
        
        initial, extra = self._parse_comtrade_data(year=year, tMarket=tMarket)
        
        checked_markets = [initial_code]
        
        extra.reset_index(inplace=True)
        
        iteration_codes = [str(x) for x in extra['Partner Code']]
        
        #print(iteration_codes)
        
        found_markets.extend(iteration_codes)
        
        #print(found_markets)
        
        extra_market_production = sum([self._get_production_amount(x, year) for x in iteration_codes])
        
        #print(extra_market_production)
        
        i = 1
        
        to_check = [x for x in found_markets if x not in checked_markets]
        
        while len(to_check) > 0 and i<stop_n:
            
            i += 1
            print("-"*100)
            print ('Iteration {}... Checking {}\nReference market production = {}'.format(i, ", ".join([self._get_country_name(x) for x in iteration_codes]), extra_market_production))
            print("-"*100)
            
            new_extras = []
            
            for m in iteration_codes:
            
                if m not in checked_markets:

                    checked_markets.append(m)

                    this_market, this_extra = self._parse_comtrade_data(importer_code=m, market_production = extra_market_production, year = year, tMarket=tMarket)

                    this_extra.reset_index(inplace=True)
                    
                    to_add = [str(x) for x in this_extra['Partner Code'] if str(x) not in found_markets and str(x) not in new_extras]
                    
                    this_country = self._get_country_name(m)
                    
                    if len(to_add) > 0:
                        print("Adding {} to market (via {})".format(", ".join([self._get_country_name(x) for x in to_add]), this_country))
                    else:
                        print("No new markets from {}".format(this_country))

                    new_extras.extend(to_add)

                    #print(new_extras)
                    
            extra_market_production += sum([self._get_production_amount(x, year) for x in new_extras])
            
            iteration_codes = new_extras

            found_markets.extend(new_extras)
            
            to_check = [x for x in found_markets if x not in checked_markets]
        
        
        market_names = [self._get_country_name(x) for x in found_markets]
               
        return found_markets, market_names
                
        
    def multi_year_markets(self, years=None, tMarket=None, tYear=None):
        
        if years is None:
            years = self.years
            
        if tYear is None:
            tYear = self.tYear
        
        if tMarket is None:
            tMarket = self.tMarket
        
        print('Finding markets for {}, for {} to {} (inclusive). tMarket = {}%, tYear = {}%\n'.format(self.market, years[0], years[-1], tMarket*100, tYear*100))
        
        data = {}
        
        for y in years:
            data[y] = self.find_markets(year=y, tMarket=tMarket)
        
        print("="*100)
        
        self.time_series = data
        self.tMarket_tYear_suppliers = self._market_by_year(tYear)
        
        print("="*100)
                    
        return None
    
    def _market_by_year(self, tYear=None):
        
        if tYear is None:
            tYear = self.tYear
        
        if hasattr(self, 'time_series'):
            ts = self.time_series
            years = len(ts)
            countries = []
            for k, v in ts.items():
                countries.extend(v[1])
            count = Counter(countries)
            
            threshold = years*tYear
            
            keep = []
            
            print("\nThe following countries were found\n\n")
            
            count_data = [["Name","Count", "Keep?"]]
            
            for c in count:
                
                this_country = c
                this_count = count[c]
                this_keep = "No"
                
                if this_count >= threshold:
                    keep.append(c)
                    this_keep = "Yes"
                
                count_data.append([this_country, this_count, this_keep])
            
            col_width = max(len(str(word)) for row in count_data for word in row) + 2  # padding
            for row in count_data:
                print ("".join(str(word).ljust(col_width) for word in row))
            
            print("\nThese countries were kept\n{}".format(", ".join(keep)))
            
            
            return keep
        else:
            print('Need to run multi_year_markets first')
            
            return None
            
        
        