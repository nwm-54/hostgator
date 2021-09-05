import pandas as pd
import numpy as np
from datetime import datetime
import csv
#update Jul 05 2021, add westleywasteway, spanish

def main():
    stations = [{'station_name': 'Hospitalcreek', 'stats_filename':'/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/hospitalcreek/output/hospitalcreek_stats.csv', 'obs_filename':'/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/hospitalcreek/output/hospitalcreek_obs.csv'}, 
                {'station_name': 'Ingramcreek', 'stats_filename':'/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/ingramcreek/output/ingramcreek_stats.csv', 'obs_filename':'/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/ingramcreek/output/ingramcreek_obs.csv'},
                {'station_name': 'Westleywasteway', 'stats_filename':'/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/westleywasteway/output/westleywasteway_stats.csv', 'obs_filename':'/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/westleywasteway/output/westleywasteway_obs.csv'},
                {'station_name': 'Spanish', 'stats_filename':'/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/spanish/output/spanish_stats.csv', 'obs_filename':'/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/spanish/output/spanish_obs.csv'}]
    
    all_dfs = []
    
    for station in stations:
        df = pd.read_csv(station['stats_filename'])
        new_columns = ['Date','DayEcAvg:Value','DayEcMin:Value','DayEcMax:Value','DayEcStd:Value',
                      'DaySaltLoadAvg:Value','DaySaltLoadMin:Value','DaySaltLoadMax:Value','DaySaltLoadStd:Value',
                      'DayFlowAvg:Value','DayFlowMin:Value','DayFlowMax:Value','DayFlowStd:Value']
        source_column = [station['station_name'] for i in range(df.shape[0])]
        df.columns = new_columns
        for idx,col in enumerate(new_columns[1:]):
            source_col_name = col.split(':')[0]+':Value_Source'
            df.insert(loc=idx*2+2, column=source_col_name, value=source_column)
            
        #interpolate + format float number
        df.interpolate(inplace=True)
        df.fillna(method='bfill', inplace=True)
        df = df.round({'DayEcAvg:Value': 0,'DayEcMin:Value': 0,'DayEcMax:Value':0,'DayEcStd:Value':0,
                        'DaySaltLoadAvg:Value': 1,'DaySaltLoadMin:Value': 1,'DaySaltLoadMax:Value':1,'DaySaltLoadStd:Value':1,
                        'DayFlowAvg:Value': 1,'DayFlowMin:Value': 1,'DayFlowMax:Value':1,'DayFlowStd:Value':1})
        
        df.to_csv(station['obs_filename'],index=False)

        all_dfs.append(df)
        
    result = all_dfs[0]
    for df in all_dfs[1:]:
        result = result.merge(df, on='Date', how='outer')
    
    startDate = datetime.strptime(result['Date'].iloc[0], "%m/%d/%Y").strftime("%m-%d-%Y")
    endDate = datetime.strptime(result['Date'].iloc[-1], "%m/%d/%Y").strftime("%m-%d-%Y")
    obs_filename = '/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/warmf_parsing/OBS_SJVDA_'+startDate+'_'+endDate+'.csv'
    
    fields = ['Date']
    for station in stations:
        for i in range(24):
            fields.append(station['station_name'])
    with open(obs_filename, 'w') as csvfile: 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(fields)    
        
    result.rename(columns={result.columns[0]: "" }, inplace=True)
    result.to_csv(obs_filename, index=False, mode='a')
    # result.to_csv('/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/warmf_parsing/OBS_SJVDA.csv', index=False)
    
if __name__ == '__main__':
    main()
          