# from utils_04032021 import *
from utils_07052021 import *
from datetime import date

# Update Jun 19,2021: 
# Update Jul 5,2021: add WestleyWasteway, spanish
def main():

	hospitalcreek_info = {'name': 'hospitalcreek',
					'salt_load_const': 0.64,
					'flow_const': 3.33,
					'weir_width': 4.45 ,
					'offset': -0.025,
					'flow_power': 1.5}

	ingramcreek_info = {'name': 'ingramcreek',
					'salt_load_const': 0.64,
					'flow_const': 3.33,
					'weir_width': 10.0,
					'offset': -0.001,
					'flow_power': 1.5}

	westleywasteway_info = {'name': 'westleywasteway',
					'salt_load_const': 0.64,
					'flow_const': 0,
					'weir_width': 0,
					'offset': 0,
					'flow_power': 0}    
					
    # marshall_info = {'name': 'westleywasteway',
				# 	'salt_load_const': 0.64,
				# 	'flow_const': 0,
				# 	'weir_width': 0,
				# 	'offset': 0,
				# 	'flow_power': 0}
	spanish_info = {'name': 'spanish',
					'salt_load_const': 0.64,
					'flow_const': 0,
					'weir_width': 0,
					'offset': 0,
					'flow_power': 0}


	for station_info in [hospitalcreek_info, ingramcreek_info, westleywasteway_info, spanish_info]:
		dirname_in = '/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/'+station_info['name']+'/'
		dirname_stats_output = '/home/nwtquinn//public_ftp/incoming/sjvda-realtime.org/SJVDA/'+station_info['name']+'/output/'+station_info['name']+'_stats.csv'
		

		today = date.today()
# 		start_date = Date(8,14,2020)
# 		end_date = Date(4,29,2021)
# 		cur_date = start_date
# 		while cur_date<=end_date:
		cur_date = Date(today.month, today.day, today.year)-1
						
		cur_date_output_file_name = file_name_from_date(cur_date)
					
		dirname_raw_output = '/home/nwtquinn//public_ftp/incoming/sjvda-realtime.org/SJVDA/'+station_info['name']+'/output/daily_raw_data/'+station_info['name']+cur_date_output_file_name+'.csv'
						
		my_station = Station(cur_date, station_info, dirname_in)
					
		raw_data = my_station.collect_raw_data()
		
		write_to_csv(raw_data,output=dirname_raw_output, mode='w')
		daily_stats = my_station.calc_daily_stats()
# 		print("write to csv ", daily_stats)
		if date_not_inserted(daily_stats,dirname_stats_output):
			print("write to csv ", daily_stats)
			write_to_csv(daily_stats,output=dirname_stats_output, mode='a')
# 		cur_date +=1
	
	


if __name__ == "__main__":
	main()