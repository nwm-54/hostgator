#copy from hostgator_preprocess
#Aug 15, 2021
# collect 15-minutes raw csv data from each station and merge them together before importing with hecdss-vue scripting

import numpy as np
import pandas as pd
import glob
from datetime import datetime

station_names = [ 'hospitalcreek','ingramcreek','westleywasteway', 'marshall', 'spanish' ]

def main():
    for station_name in station_names:
        input_path = f'/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/{station_name}/'

        print('Merging station', station_name)
        inputFiles = glob.glob(input_path+'*.csv')
        all_dfs = []
        
        for file_name in inputFiles:
            if station_name in file_name.split('/')[-1]:
                print('filename', file_name)
                df = pd.read_csv(file_name,
                                names=["Date", "Time", "Parameter", "Value","Unit", "Status"])
                df = df[:-1]
                try:
                    df['DateTime'] = df.apply(lambda row: datetime.strptime(str(row['Date'])+str(row['Time']), '%m/%d/%Y%H:%M:%S'), axis=1)
                    all_dfs.append(df)
                except KeyError:
                    print('filename empty ', filename)
                
            
        merge_df = pd.concat(all_dfs, axis=0)
        merge_df.sort_values(by='DateTime',inplace=True,ascending=True)
        merge_df = merge_df.drop('DateTime', 1)

        output_path = f'/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/{station_name}/output/{station_name}_merged.csv'
        merge_df.to_csv(output_path, index=False)
    
if __name__ == '__main__':
    main()