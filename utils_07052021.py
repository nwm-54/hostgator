import re
import csv
import os
from math import sqrt
from copy import deepcopy
from functools import reduce

#Update Jun 19, 2021: change temp range from C to F
#Update Jul 5, 2021: add marshall, spanish, westleywasteway with reported flow value
'''
	HELPER FUNCTIONS	
'''
def write_to_csv(data, output='/home/vitran/data_test_jan18/missing_sensor_data.csv',mode='w'):
	''' Write/Add 2-level nested list to a given csv file. 
		Default mode are writing new csv file.

	--Parameters:
		data: list, two level nested list. ex: [[1,2,3],[4,5,6]]
		output: str, file path for .csv
		mode: char, 'w' for writing new file and 'a' for appending new data to an existing file
	
	--Return:
		None
	'''
	if mode == 'w':
		with open(output,'w') as outfile:
			writer = csv.writer(outfile)
			writer.writerows(data)
		outfile.close()
	elif mode == 'a':
		#only applies to daily report csv file
		# date_to_add = data[0][0]
		# all_dates_reversed = []
		# with open(output,'r') as outfile:
		# 	reader = csv.reader(outfile)
		# 	next(reader)
		# 	for line in reversed(list(reader)):
		# 		all_dates_reversed.append(line)
		# outfile.close()

		# if len(all_dates_reversed) != 0:
		# 	index = 0
		# 	cur_date_str = all_dates_reversed[index][0].split('/')
		# 	cur_date = Date(cur_date_str[0], cur_date_str[1],cur_date_str[2])

		# 	while date_to_add < cur_date and index<len(all_dates_reversed):
		# 		cur_date_str = all_dates_reversed[index][0].split('/')
		# 		cur_date = Date(cur_date_str[0], cur_date_str[1],cur_date_str[2])
		# 		index += 1
		# if len(all_dates_reversed) == 0 or date_to_add != cur_date: # if ==: the date is already recorded
		with open(output,'a') as outfile:
			writer = csv.writer(outfile)
			writer.writerows(data)
		outfile.close()
	else:
		raise Exception("invalid mode, chose 'w' or 'a'")		
def date_not_inserted(data, output):
	''' Check wheter the date was included in the output file or not

	--Parameters:
		data: list, two level nested list with Date objects at 0-indexed. ex: [[<Date object>],[<Date object>]]
		output: str, file path for .csv
	
	--Return:
		True if the date was not found in .csv file
	'''
	date_to_add = data[0][0]
	all_dates_reversed = []
	with open(output,'r') as outfile:
		reader = csv.reader(outfile)
		next(reader)
		for line in reversed(list(reader)):
			if line != "" and line !='\n':
				all_dates_reversed.append(line)
	outfile.close()

	if len(all_dates_reversed) != 0:
		index = 0
		cur_date_str = all_dates_reversed[index][0].split('/')
		cur_date = Date(cur_date_str[0], cur_date_str[1],cur_date_str[2])

		while index<len(all_dates_reversed): #date_to_add < cur_date and 
			cur_date_str = all_dates_reversed[index][0].split('/')
			cur_date = Date(cur_date_str[0], cur_date_str[1],cur_date_str[2])
			if date_to_add == cur_date:
				return False
			index += 1
	return True
# 	return len(all_dates_reversed) == 0 or date_to_add != cur_date  

def safe_cast(val, to_type=float, default=None):
	''' Cast value into given type
		Default type is float

	--Parameters:
		val: data to cast
		to_type: data type
		default: optional, default value if casted unsucessfully
	--Return:
		data of casted type
	'''
	try:
		num = to_type(val)
		if num < 0: return default
		else: return num
	except (ValueError, TypeError):
		return default

def is_valid_data(val):
	''' Check if val is of float type and is positive

	--Parameters:
		val: data to check
	
	--Return:
		True if data of type float and positive
	'''
	x = safe_cast(val, float)
	return (x is not None) and x>0.0

def stringify_datetime(num):
	''' For datetime data: Convert number data to 2-digit string data

	--Parameters:
		num: int, number to be converted
	
	--Return:
		num of string type if length >=2
	'''
	num_str = str(num)
	if len(num_str)<2:
		num_str = '0' + num_str
	return num_str

def sort_extend(lst, key=None):
	''' Sort list by timestamp as second element in nested list
		Ex: [[, 03:50:00,],[, 03:15:00,]] -> [[, 03:15:00,],[, 03:50:00,]]

	--Parameters:
		lst: list of timestamp data. Ex: [[, 03:50:00,],[, 03:15:00,]]
	
	--Return:
		Sort list of timestamp in ascending order. 
	'''
	def get_seconds(x):
		# print('in get seconds: ',x)
		hms = x[1].split(':')
		# hms = x[2].split(':') ## use when dir is index 0
		return hms[0]*60*60+hms[1]*60+hms[2]
	ans = sorted(lst, key = lambda x: get_seconds(x))
	return ans

def file_name_from_date(date):
	''' Making combined date string from Date object. 
		Ex: Jan 13th, 2020 -> 20200113

	--Parameters:
		date: Date, date to parse. 
	
	--Return:
		str, combined date string. 
	'''
	d,m,y = str(date.get_day()),str(date.get_month()),str(date.get_year())
	if len(d) == 1: d = '0'+d
	if len(m) == 1: m = '0'+m
	return str(y)+str(m)+str(d)

def generate_fix_regex(cur_date, station):
	''' Generating fix part of csv file name of given station at specific date
		Ex: Jan 13th, 2020 at ingramcreek -> ingramcreek_20200113

	--Parameters:
		cur_date: Date, date 
		station: str, name of station
	
	--Return:
		str, file name. 
	'''
	prev_date = station+'_'+file_name_from_date(cur_date.prev_date())
	this_date = station+'_'+file_name_from_date(cur_date)
	next_date = station+'_'+file_name_from_date(cur_date.next_date())
	print(this_date)
	return prev_date, this_date, next_date

def is_correct_date(s, cur_date):
	''' Check whether date from string matched the Date object

	--Parameters:
		s: str, date string of format mm/dd/yyyy
		cur_date: Date, other date
	
	--Return:
		True of date string matches the Date object
	'''
	check = s.split('/')
	test = Date(check[0], check[1], check[2])
	return test == cur_date

def get_file_path(dirname,cur_date,station):
	''' Generating raw data file name in given directory 

	--Parameters:
		dirname: str, directory
		cur_date: Date, date to collect raw data
		station: str, station name
	
	--Return:
		file_name in string. 
	'''
	all_files = os.listdir(dirname)
# 	if 'westleywasteway' not in dirname: #WHY ?
	prev_datetime, this_datetime, next_datetime = generate_fix_regex(cur_date, station)
	prev_pattern = re.compile(r'%s00[0-9]+\.csv' % re.escape(prev_datetime)) 
	main_pattern = re.compile(r'%s(0[0-9]|1[0-9]|2[0-9])[0-9]+\.csv' % re.escape(this_datetime))
	next_pattern = re.compile (r'%s00[0-9]+\.csv' % re.escape(next_datetime)) 
	
	all_files = [dirname+i for i in all_files if (main_pattern.match(i) or next_pattern.match(i) or prev_pattern.match(i))]

	return all_files

def deep_copy(object):
	''' Return deep copy of an object
		Used for user-defined class object

	--Parameters:
		object: object to copy
	
	--Return:
		Newly copied object
	'''
	return deepcopy(object)

def decimal_conversion(f, num_decimal):
	''' Convert float to str with the num_decimal number of decimal place

	--Parameters:
		f: float number
		num_decimal: number of decimal place
		
	--Return:
		str with the num_decimal number of decimal place
	'''
	fm = '.'+str(num_decimal)+'f'
	return format(f, fm)


class Station:
	"""
    A class used to represent a station

    ...

    --Attributes:
    cur_date : Date, date of interest
    dirname_input: str, directory to get raw input .csv files
    station_name : str, name of the station
    salt_load_const: float, salt load factor to convert EC to salt load. Ex: satl load = ec*salt loat factor
    flow_const, weir_width, offset, flow_power: float, factors in flow equation. flow = flow_const*weir_width*((stage-offset)^flow_power)
    temp_name: str, name of parameters in .csv file. Ex: HO_EC_IS
    report: list, list of all raw data points from a given day
       
    """
	def __init__(self, cur_date, station_info, dirname_in):
		self.cur_date = cur_date
		self.dirname_input = dirname_in
		# self.dirname_raw_output = dirname_in_out['raw_output']
		# self.dirname_stats_output = dirname_in_out['stats_output']
		self.station_name = station_info['name']
		self.salt_load_const = station_info['salt_load_const']
		self.flow_const = station_info['flow_const']
		self.weir_width = station_info['weir_width']
		self.offset = station_info['offset']
		self.flow_power = station_info['flow_power']
		self.stage_stations = ['hospitalcreek', 'ingramcreek','marshall']
		self.temp_name, self.ec_name, self.stage_name = self.get_temp_ec_stage_name()
		self.report = self.init_report()

	def init_report(self):
		''' Initializing raw data points list

		--Parameters:
			None

		--Return:
			a nested list
		'''
		if self.station_name in self.stage_stations:
			report = [['date(mm/dd/yyyy)','time(h:m:s)','temp(F)','ec(uS/cm)','stage(ft)']]
		else:
			report = [['date(mm/dd/yyyy)','time(h:m:s)','temp(F)','ec(uS/cm)','flow(cfs)']]
		for h in range(0,24,1):
			for m in range(0,60,15):
				h_str = stringify_datetime(h)
				m_str = stringify_datetime(m)
				timestamp = h_str + ':' + m_str + ':00'
				report.append([self.cur_date,timestamp,None, None, None])
		return report

	def get_temp_ec_stage_name(self):
		''' Getting parameters name from station name

		--Parameters:
			None
			
		--Return:
			temp_name: str, <station>_temp_IS
			ec_name: str, <station>_EC_IS
			stage_name: str, <station>_stage_DA
		'''
		temp_name = ec_name = stage_name = None
		if self.station_name == 'hospitalcreek':
			ec_name = 'HO_EC_IS'
			stage_name = 'HO_stage-DA'
			temp_name = 'HO_temp_IS' #'HO_PTemp_DA'
		elif self.station_name == 'ingramcreek':
			ec_name = 'IN_EC_IS'
			stage_name = 'IN_stage-DA'
			temp_name = 'IN_temp_IS' #'IN_PTemp_DA'
		elif self.station_name == 'westleywasteway':
			ec_name = 'WE_EC_IS'
			stage_name = 'WE-flow-SK'#for spanish, and westleywasteway: stage var reports flow value
			temp_name = 'WE_temp_IS'
		elif self.station_name == 'marshall':
			ec_name = 'MA_EC_IS'
			stage_name = 'MA_stage-DA'
			temp_name = 'MA_temp_IS'
		elif self.station_name == 'spanish':
			ec_name = 'SP_EC_IS'
			stage_name = 'SP-SK-flow'#for spanish, and westleywasteway: stage var reports flow value
			temp_name = 'SP-temp-IS' 
		return temp_name, ec_name, stage_name
		
	def within_range(self, rows):
		value = safe_cast(rows[3], float)
		if isinstance(value, float):
			return (self.ec_name in rows and value>=150 and value<=5000) or (self.stage_name in rows and value>=0) or (self.temp_name in rows and value>=40.0 and value<=100.0) 
		return False

	def report_helper(self, h,m_idx, rows):
		''' Check whether a data point is already found and populate list with raw data points of a given date

		--Parameters:
			h: int, hour
			m_index: int, minute order
			rows: data row read for .csv raw data files
			
		--Return:
			a nested list
		'''
		report_index = h*4+m_idx+1
		self.report[report_index][1] = rows[1]
		if self.within_range(rows):
			if self.temp_name in rows:
				if self.report[report_index][2] is not None: 
					print('{} {} temp is repeated'.format(rows[0], rows[1]))
					print(self.report[report_index][2])
					print(rows[3])
				self.report[report_index][2] = rows[3] #safe_cast(rows[3], float) #
			elif self.ec_name in rows:
				if self.report[report_index][3] is not None: 
					print('{} {} ec is repeated'.format(rows[0], rows[1]))
					print(self.report[report_index][3])
					print(rows[3])
				self.report[report_index][3] = rows[3] #safe_cast(rows[3], float) #
			elif self.stage_name in rows:
				if self.report[report_index][4] is not None:
					print('{} {} stage is repeated'.format(rows[0], rows[1]))
					print(self.report[report_index][4])
					print(rows[3])
				self.report[report_index][4] = rows[3] #safe_cast(rows[3], float) #

	def salt_load_calc(self, x):
		''' Calculate salt load from ec data

		--Parameters:
			x: float, EC data
			
		--Return:
			salt load (uS/cm): float
		'''
		return x*self.salt_load_const

	def flow_calc(self, x):
		''' Calculate flow from stage data

		--Parameters:
			x: float, stage
			
		--Return:
			flow: float
		'''
		if self.station_name in self.stage_stations:
			return self.flow_const*self.weir_width*((x-self.offset)**self.flow_power)
		else: # westleywasteway, spanish
			return x

	def stddev(self, lst):
		''' Calculate standard deviation from a given list

		--Parameters:
			lst: list, list of data 
			
		--Return:
			standard deviation: float
		'''
		mean = float(sum(lst)) / len(lst)
		return sqrt(float(reduce(lambda x, y: x + y, map(lambda x: (x - mean) ** 2, lst))) / len(lst))

	def collect_raw_data(self):
		''' Collect raw data points of a given day

		--Parameters:
			None
			
		--Return:
			list of 96 data points of a given day
		'''
		all_files = get_file_path(self.dirname_input,self.cur_date,self.station_name)
		min_to_index = [0,15,30,45]
		# print(self.cur_date)

		for file_name in all_files:
			with open(file_name, 'r') as infile:
				csvreader = csv.reader(infile)
				rows = next(csvreader)

				for rows in csvreader:
					# 08/14/2020,15:15:00,HO_EC_IS,-99999,,B
					if len(rows)>3 and is_correct_date(rows[0], self.cur_date):
						hm = rows[1].split(':')
						h,m = int(hm[0]), int(hm[1])
						
						
						if m in min_to_index: 
							self.report_helper(h, min_to_index.index(m), rows)

		return self.report
		# write_to_csv(self.report,output=self.dirname_raw_output, mode='w')
		

		
	def calc_daily_stats(self):
		''' Calculate daily salt load, flow and generate its statistics

		--Parameters:
			None
			
		--Return:
			average daily salt load, flow and its statistics (mean, min, max, stddev)
		'''
		ec_daily = []
		stage_daily = [] 
		for timestamp in self.report:
			if is_valid_data(timestamp[3]):
				ec_daily.append(safe_cast(timestamp[3],float))
			if is_valid_data(timestamp[4]):
				stage_daily.append(safe_cast(timestamp[4],float))

		# end of a day
		salt_load_daily = []
		flow_daily = []
		daily_stats = [[deep_copy(self.cur_date), 
				None, None, None, None,
				None, None, None, None, 
				None, None, None, None]]
		if ec_daily: 
			#add ec
			daily_stats[0][1:5] = [decimal_conversion(sum(ec_daily)/len(ec_daily),0), decimal_conversion(min(ec_daily),0), decimal_conversion(max(ec_daily),0),decimal_conversion(self.stddev(ec_daily),0)]
				
			#add salt load
			salt_load_daily = list(map(self.salt_load_calc,ec_daily))
			daily_stats[0][5:10] = [decimal_conversion(sum(salt_load_daily)/len(salt_load_daily),1), decimal_conversion(min(salt_load_daily),1), decimal_conversion(max(salt_load_daily),1),decimal_conversion(self.stddev(salt_load_daily),1)]
		if stage_daily:
			#add stage
			flow_daily = list(map(self.flow_calc, stage_daily))
			daily_stats[0][9:] = [decimal_conversion(sum(flow_daily)/len(flow_daily),1), decimal_conversion(min(flow_daily),1), decimal_conversion(max(flow_daily),1), decimal_conversion(self.stddev(flow_daily),1)]

		return daily_stats
		# write_to_csv(daily_stats,output=self.dirname_stats_output, mode='a')

class Date:
	def __init__(self, m,d,y):
		"""
	    A class used to represent Date

	    ...

	    --Attributes:
	    date
	    month
	    year
	    days_per_month
	    	   
	    """
		if isinstance(m,str): m=int(m)
		if isinstance(d,str): d=int(d)
		if isinstance(y,str): y=int(y)
		self.date = d
		self.month = m
		self.year = y
		self.days_per_month = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
	
	def get_day(self):
		''' 
		--Return: 
			int, date of the Date object
		'''
		return self.date
	
	def get_month(self):
		''' 
		--Return: 
			int, month of the Date object
		'''
		return self.month
	
	def get_year(self):
		''' 
		--Return: 
			int, year of the Date object
		'''
		return self.year
	
	def __repr__(self): 
		''' 
		Overriding printing operation of Date object
		'''
		return "%02d/%02d/%04d" % (self.month, self.date, self.year)

	def __str__(self):
		''' 
		Overriding printing operation of Date object
		'''
		return "%02d/%02d/%04d" % (self.month, self.date, self.year)

	def check_bound_of_month(self, num_days, mode='end'): #1-indexed/ not check leap year yet
		''' Check if adding days to current date converts it to the start/last date of the month

		--Parameters:
			num_days: int, number of days
			mode: str, 'start' or 'end'
			
		--Return:
			True if current date + num_days is the start/last date of the month
		'''
		if mode == 'end':
			return (self.date+num_days) > self.days_per_month[self.month]
		elif mode == 'start':
			return (self.date-num_days) <= 0
	
	def __add__(self, num_days):
		''' 
		Overriding + operation of Date object
		'''
		if (not self.check_bound_of_month(num_days, mode='end')):
			self.date += num_days
			return self
		while (self.check_bound_of_month(num_days, mode='end')) or (num_days):
			leftover_days = self.days_per_month[self.month]-self.date
			num_days -= (leftover_days+1)
			self.month += 1
			if self.month == 13:
				self.month = 1
				self.year +=1
			self.date = 1
		return self

	def __sub__(self, num_days):
		''' 
		Overriding - operation of Date object
		'''
		if (not self.check_bound_of_month(num_days, mode='start')):
			self.date -= num_days
			return self
		while (self.check_bound_of_month(num_days, mode='start')):
			num_days -= self.date
			self.month -= 1
			if self.month == 0:
				self.month = 12
				self.year -=1
			self.date = self.days_per_month[self.month]
		return self
	
	def __lt__(self, other):
		''' 
		Overriding < operation of Date object
		'''
		if self.year > other.get_year():
			return False
		elif self.year == other.get_year():
			if self.month > other.get_month():
				return False
			elif self.month == other.get_month():
				return self.date < other.get_day()
			else:
				return True
		else: 
			return True
	def __le__(self, other):
		''' 
		Overriding <= operation of Date object
		'''
		if self.year > other.get_year():
			return False
		elif self.year == other.get_year():
			if self.month > other.get_month():
				return False
			elif self.month == other.get_month():
				return self.date <= other.get_day()
			else:
				return True
		else: 
			return True
	
	def __eq__(self, other):
		''' 
		Overriding = operation of Date object
		'''
		return self.date==other.get_day() and self.month==other.get_month() and self.year==other.get_year()
	
	def next_date(self):
		''' 
		--Parameters:
			None
			
		--Return:
			Date, next date of current date
		'''
		ans = deepcopy(self)
		return ans+1
	
	def prev_date(self):
		''' 
		--Parameters:
			None
			
		--Return:
			Date, previous date of current date
		'''
		ans = deepcopy(self)
		return ans-1