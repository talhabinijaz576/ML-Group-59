import requests
import pandas as pd
import numpy as np
import os
import json
import threading
import time


class Scraper:
    
    API_URL = "http://www.spatialjusticetest.org/api5.php?fLat={}&fLon={}"#&sGeo=county&sCompare=off&iZoom=4&iPolygon_compare=0&iPolygon=1&iComparedefault=1"

    OUTPUT_FOLDER = "data"
    OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "demographics.csv")
    ZIPCODES_FILE = os.path.join(OUTPUT_FOLDER, "zipcodes.csv") 
    ZILLOW_FILE = os.path.join(OUTPUT_FOLDER, "zillow.csv") 
    
    n_threads_start = threading.activeCount()
    
    def __init__(self):

        zillow = pd.read_csv(self.ZILLOW_FILE)
        zipcodes = pd.read_csv(self.ZIPCODES_FILE, sep=";")
        zipcodes = zipcodes[["ZipCode", "Latitude", "Longitude"]]


        self.columns = ["ZipCode", "Income", "Income Change", "Population", "Density", "White Percentage"]

        if(os.path.exists(self.OUTPUT_FILE)):
            df = pd.read_csv(self.OUTPUT_FILE)
            df = df[self.columns]
            df["ZipCode"] = df["ZipCode"].astype(int).astype(str)
        else:
            df = pd.DataFrame(columns=self.columns)
    
        df = pd.DataFrame(columns=self.columns)
        self.data = df.astype(float).values
        
        zipcodes_to_scrape = zipcodes.merge(zillow[["ZipCode"]])
        zipcodes_to_scrape = zipcodes_to_scrape.drop_duplicates(subset=['ZipCode'])
        zipcodes_to_scrape = zipcodes_to_scrape.sample(frac=1)
        self.zipcodes_to_scrape = zipcodes_to_scrape.reset_index()
        
        
    def ScrapeZipcode(self, zipcode): 
        
        zipcode_str = str(int(zipcode["ZipCode"])).split(".")[0]
        if (zipcode_str in self.data[:, 0]):
            return
        
        try:
            url = self.API_URL.format(zipcode["Latitude"], zipcode["Longitude"])
            print(url)
            r = requests.get(url)
            if(r.status_code==200):
                info = json.loads(r.content)
                row = [zipcode_str , info["income"], info["income_change"], info["pop"], info["density"], info["white"]]
                self.data = np.vstack((self.data, row))
                n_done = self.data.shape[0]
                if(n_done%100 == 0):
                    print(n_done, "zipcodes done")
                    self.SaveDataset()
        except Exception as e:
            print(e)
            pass
            
                    
    def ScrapeAll(self, MAX_THREADS = 10):
            
        print("Zipcodes to process: ", self.zipcodes_to_scrape.shape[0])
            
        for _, zipcode in self.zipcodes_to_scrape.iterrows():
            
            while( threading.activeCount() > MAX_THREADS ):
                time.sleep(0.1)
            threading.Thread(target=self.ScrapeZipcode, args=(zipcode,)).start()
            
        self.SaveDataset()

                
    def SaveDataset(self):
        df = pd.DataFrame(self.data, columns = self.columns)
        df.to_csv(self.OUTPUT_FILE, index=False)
                
scraper = Scraper()
scraper.ScrapeAll()
scraper.SaveDataset()



