import os
import shutil
import glob
import csv
  
# This file move data file from MarshallSpanish and Westleywasteway station in home directory to their respective folder (+split Marshal and Spanish as well ) - RUN THIS HOURLY TO MOVE NEW DATA TO STATION FOLDER FOR DAILY CALCULATION
def main():
    path = '/home/nwtquinn/'
    extension = 'csv'
    os.chdir(path)
    files = glob.glob('*.{}'.format(extension))
    for file in files:
        if 'WestleyWasteway' in file:
            dest = '/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/westleywasteway/westleywasteway_'+file.split('_')[-1]
            shutil.move(path+file, dest)
        elif 'marshallspanish' in file:
            marshall_csv = []
            spanish_csv = []
            with open(file, 'r') as csvfile:
                csvreader = csv.reader(csvfile)
                for row in csvreader:
                    if len(row)>1:
                        if 'MA' in row[2]:
                            marshall_csv.append(row)
                        elif 'SP' in row[2]:
                            spanish_csv.append(row)
            marshall_out_file =  '/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/marshall/marshall_'+file.split('_')[-1]
            with open(marshall_out_file, 'w') as csvfile: 
                csvwriter = csv.writer(csvfile) 
                csvwriter.writerows(marshall_csv)
                
            spanish_out_file =  '/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/spanish/spanish_'+file.split('_')[-1]
            with open(spanish_out_file, 'w') as csvfile: 
                csvwriter = csv.writer(csvfile) 
                csvwriter.writerows(spanish_csv)
                
            dest = '/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/marshallspanish/marshallspanish_'+file.split('_')[-1]
            shutil.move(path+file, dest)
            # os.remove(path+file)

  
# Driver Code
if __name__ == '__main__':
    main()