from selenium import webdriver
import numpy as np
import pandas as pd
import time
import os
import shutil
import requests

TEMP_FOLDER = "TEMP"
OUTPUT_FOLDER = "data"
CHROMEDRIVER_PATH = "chromedriver.exe"
URL = "'https://www.zillow.com/research/data/'"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "zillow.csv")

if(not os.path.exists(OUTPUT_FOLDER)):
    os.mkdir(OUTPUT_FOLDER)
    
def ScrapeZillow():
    print("Scraping started")
    if(os.path.exists(TEMP_FOLDER)):
        shutil.rmtree(TEMP_FOLDER)
    os.mkdir(TEMP_FOLDER)
    
    driver = webdriver.Chrome(CHROMEDRIVER_PATH)
    driver.get(URL)
    
    dropdown_type_id = "median-home-value-zillow-home-value-index-zhvi-dropdown-1"
    
    while(len(driver.find_elements_by_id(dropdown_type_id))==0):
        time.sleep(0.2)
    
    dropdown_type = driver.find_element_by_id(dropdown_type_id)
    options = dropdown_type.find_elements_by_xpath("*")
    
    zillow_files = []
    for i in range(len(options)):
        dropdown_type = driver.find_element_by_id(dropdown_type_id)
        dropdown_type.click()
        option = dropdown_type.find_elements_by_xpath("*")[i]
        name = option.text
        if( ("All" not in name) and ("Forecast" not in name)):
            
            option.click()
            time.sleep(0.1)
            
            dropdown_geo = driver.find_element_by_id("median-home-value-zillow-home-value-index-zhvi-dropdown-2")
            dropdown_geo.click()
            
            zipcode_element = dropdown_geo.find_element_by_xpath("//option[contains(text(), 'ZIP Code')]")
            file_url = zipcode_element.get_attribute("value")
            
            name = name[5:].split(" Time")[0].replace("/", "-")
            save_path = os.path.join(TEMP_FOLDER, name+" (ZILLOW).csv")
            zillow_files.append(save_path)
            
            print("Downloading {} from {} at {}".format(name, file_url, save_path))
            r = requests.get(file_url)
            with open(save_path, mode='wb') as f:
                f.write(r.content)   
            print("File downloaded")
            
    driver.quit()
    
    print("Scraping finished")
    print()
    return zillow_files
        

def ProcessDF(filepath, house_types):
    print("Processing {}".format(filepath))
    df = pd.read_csv(filepath)
    
    region_type = df["RegionType"][0]
    region_type = "ZipCode"
    
    df_house_type = os.path.basename(filepath).split(" (ZILLOW)")[0]
    
    #print(list(df.columns)[:10])
    columns_needed =  [df.columns[0] ] + [x for x in list(df.columns)[9:] if (int(x[:4])>2015)]
    
    df = df[columns_needed]
    
    columns = list(df.columns)
    columns = [region_type] + [x[:-3] for x in columns[1:]]
    
    df.columns = columns
    
    df["Price"] = df.apply(lambda x: np.median(x[1:]), axis=1)
    
    df = CalculateGrowthRate(df)
    
    df = df[["ZipCode", "Price", "Growth_YoY"]]
    
    for house_type in house_types:
        df[house_type] = 0
        #df[house_type] = df[house_type].astype(int)
        
    df_house_type = os.path.basename(filepath).split(" (ZILLOW)")[0]
    df[df_house_type] = 1
    
    print("Processing of {} completed".format(filepath))
    
    return df
    

def CalculateGrowthRate(df, ignore_last=10):
    print("Calculating growth rate")
    columns = list(df.columns)
    growth_rates = []
    for i in range(1, len(columns)-12-(ignore_last)):
        growth = ( (100*( df.iloc[:, i+12] - df.iloc[:, i] )) / df.iloc[:, i] ).values
        growth_rates.append(growth)
    
    avg_growth_rates = np.vstack(growth_rates).T.mean(axis=1)
    df["Growth_YoY"] = np.round(avg_growth_rates, 2)
    
    return df


def CompileZillowDataset():

    zillow_files = [os.path.join(TEMP_FOLDER, filename) for filename in os.listdir(TEMP_FOLDER) if ("ZILLOW" in filename)]
    house_types = [os.path.basename(filepath).split(" (ZILLOW)")[0] for filepath in zillow_files]
    
    print("Compiling dataset")
    all_dfs = [] 
    for filepath in zillow_files:
        df = ProcessDF(filepath, house_types)
        all_dfs.append(df)
    
    print("Combining dataset")    
    data = np.vstack([df.values for df in all_dfs])
    df = pd.DataFrame(data=data, columns = all_dfs[0].columns)
    print("Compilation finished")   
    
    df = df[df["Price"].isnull()==False]
    
    
    df["ZipCode"] = df["ZipCode"].astype(int)
    df = df.astype(int)
    
    df.to_csv(OUTPUT_FILE, index=False)

    
    return df

#ScrapeZillow()
df = CompileZillowDataset()































    
    
