import json
import os
import pandas as pd
import random
import string
import time
import yaml
from io import StringIO

from collections import OrderedDict
import utils

class lar_gen(object):
	"""
	Contains functions to create a valid LAR and TS record
	Functions:
	- date_gen
	- random_enum
	- get_schema_val
	- get_schema_list
	- range_and_enum
	- tract_from_county
	- make_ts_row
	- make_row
	"""
	def __init__(self, lar_schema_file="2023/schemas/lar_schema.json", ts_schema_file="2023/schemas/ts_schema.json",for_frontend = False):
	#, config_file='configurations/clean_file_config.yaml', geo_config_file='configurations/geographic_data.yaml'):
		"""
		lar_schema_file: JSON file with LAR schema
		ts_schema_file: JSON file with TS schema
		config_file: configuration file holding bank identifers and LAR value configs
		geo_config_file: the FFIEC flat file with census data used in the HMDA Platform see https://github.com/cfpb/hmda-census
		"""
		print("start initialization of LAR generator")

		#load Schemas for valid values for fields for LAR and TS
		with open(lar_schema_file, 'r') as f:
			lar_schema_json = json.load(f)
		self.lar_schema_df = pd.DataFrame(lar_schema_json)

		with open(ts_schema_file, 'r') as f:
			ts_schema_json = json.load(f)
		self.ts_schema_df = pd.DataFrame(ts_schema_json)
		self.for_frontend = for_frontend
		#with open(self.geo_config["zip_code_file"], 'r') as f:
		#	self.zip_codes = json.load(f)
		#self.zip_codes.append("Exempt") #add Exempt as valid zip code
		#self.state_codes_rev = self.geo_config["state_codes_rev"]
		#cleanup unneeded variables
		#del self.geo_config
		del lar_schema_json
		del ts_schema_json
		print("LAR generator initialization complete")

	def date_gen(self, activity_year, valid=True):
		"""Generates and returns a semi-valid date string or an invalid date string. Does not check days per month."""
		months = list(range(1,13))
		days = list(range(1,32))
		if valid:
			valid_date = False
			while valid_date == False:
				date = str(activity_year)+str(random.choice(months)).zfill(2)+str(random.choice(days)).zfill(2)
				try:
					time.strptime(date,'%Y%m%d')
					valid_date = True
				except:
					valid_date = False
		# else:
			# date = str(lar_file_config["calendar_year"]["value"])+str(16)+str(33)
		return date

	def get_schema_val(self, schema="LAR", position=0, item=0, field=None):
		"""Returns a value from the valid_vals list in the schema for the named field. Default is the first value in the list."""
		if not field:
			raise ValueError("must specify which field")
		if schema=="LAR":
			return self.lar_schema_df.valid_vals[self.lar_schema_df.field==field].iloc[position][item]
		elif schema=="TS":
			return self.ts_schema_df.valid_vals[self.ts_schema_df.field==field].iloc[position][item]
		else:
			pass

	def get_schema_list(self, schema="LAR", field=None, empty=False):
		"""
		Returns the list of valid values for the specified schema and field. 
		Optionally adds blanks to the list of values.
		"""
		
		if not field:
			raise ValueError("must specify which field")

		if schema=="LAR":
			if empty:
				schema_enums = self.lar_schema_df.valid_vals[self.lar_schema_df.field==field].iloc[0]
				schema_enums.append("")
				return schema_enums
			else: 
				return self.lar_schema_df.valid_vals[self.lar_schema_df.field==field].iloc[0]

		elif schema=="TS":
			if empty:
				schema_enums = self.ts_schema_df.valid_vals[self.ts_schema_df.field==field].iloc[0]
				schema_enums.append("")
				return schema_enums
			else:
				return self.ts_schema_df.valid_vals[self.ts_schema_df.field==field].iloc[0]

	def range_and_enum(self, field=None, rng_min=1, rng_max=100, dtype="int", empty=False):
		"""
		Returns a list of integers or floats. 
		if na is True the returned list will contain NA
		if empty is True the returned list will contain an empty string
		"""

		lst=[]
		lst = self.get_schema_list(field=field) #get NA values from schema if present
		if dtype=="int":
			for i in range(rng_min, rng_max):
				lst.append(i)
		elif dtype=="float":
			for i in range(rng_min, rng_max):
				lst.append(i*1.01)
		if empty:
			lst.append("")
		return lst

	def tract_from_county(self, county):
		"""Returns a Census Tract FIPS that is valid for the passed county."""
		valid_tracts = [tract for tract in self.tract_list if tract[:5]==county]
		return random.choice(valid_tracts)

	def make_ts_row(self, bank_file_config):
		"""Creates a TS row as a dictionary and returns it."""
		ts_row = OrderedDict()
		ts_row["record_id"] ="1"
		ts_row["inst_name"] = bank_file_config["name"]["value"]
		ts_row["calendar_year"] = bank_file_config["activity_year"]["value"]
		ts_row["calendar_quarter"] = bank_file_config["calendar_quarter"]["value"]
		ts_row["contact_name"] = bank_file_config["contact_name"]["value"]
		ts_row["contact_tel"] = bank_file_config["contact_tel"]["value"]
		ts_row["contact_email"] = bank_file_config["contact_email"]["value"]
		ts_row["contact_street_address"] = bank_file_config["street_addy"]["value"]
		ts_row["office_city"] = bank_file_config["city"]["value"]
		ts_row["office_state"] = bank_file_config["state"]["value"]
		ts_row["office_zip"] = str(bank_file_config["zip_code"]["value"])
		ts_row["federal_agency"] = bank_file_config["agency_code"]["value"]
		ts_row["lar_entries"]= str(bank_file_config["file_length"]["value"])
		ts_row["tax_id"] = bank_file_config["tax_id"]["value"]
		ts_row["lei"] = bank_file_config["lei"]["value"]
		return ts_row

 
	def make_row(self, lar_file_config, geographic_data, state_codes, zip_code_list):
		"""Make num_rows LAR rows and return them as a list of ordered dicts"""
 		
		# valid_lar_row = OrderedDict()
		lar_schema_col_names = self.lar_schema_df["field"].to_list() 
		lei = lar_file_config["lei"]["value"]
		uli = lei + utils.char_string_gen(23)
		uli = uli + utils.check_digit_gen(ULI=uli)
		uli = random.choice([uli, utils.char_string_gen(22)])
		app_date = str(self.date_gen(activity_year=lar_file_config["activity_year"]["value"]))
		year = lar_file_config["activity_year"]["value"]
		actionTakenDate = str(self.date_gen(activity_year=lar_file_config["activity_year"]["value"]))
		state = "DC"
		county = "11001"
		censusTract = "11001980000"
		
		# values_str = f"2|{lei}|{uli}|{app_date}|3|2|2|2|3|218910|5|{actionTakenDate}|1234 Hocus Potato Way|Washington|DC|14755|{county}|{censusTract}|1|13||11||KE0NW|1||||||2|2|7||||||||27|24|41|43|2||||3|2|2|2|1|1|75|44|85|0|NA|3|2|8888|8888|9||9||10|||||NA|NA|NA|NA|NA|NA|NA|NA|NA|256|29|2|2|2|2|NA|1|2|4|NA|2|2|NA|3|1|5|1||DOREBESQSW1QT58SD2OZTHQUGXLSKCAJYZ63NJE2MUIAFQL4KW6PU26YSU786GT0IMCWWKCN25Y7KU0VLU0PPKWR8G6DKWI9BANPIE9I2ZZ5XDUX0TBAY4XFRFQZF087WS9ESTAKIV5V9HSZ2VXW7J5JMGPP4CGYA51BK68T57NN4KTKJVXIQMFXBTN5E3LGKKX3LITQ4C7OPFJ|8|6|5|5|||2|2|1"
		if lar_file_config["calendar_quarter"]["value"] == 4:
			values_str = f"2|{lei}|{uli}|{year}0113|3|2|2|2|3|218910|5|{year}1010|1234 Hocus Potato Way|Washington|DC|14755|{county}|{censusTract}|1|13||11||KE0NW|1||||||2|2|7||||||||27|24|41|43|2||||3|2|2|2|1|1|75|44|85|0|NA|3|2|8888|8888|9||9||10|||||NA|NA|NA|NA|NA|NA|NA|NA|NA|256|29|2|2|2|2|NA|1|2|4|NA|2|2|NA|3|1|5|1||DOREBESQSW1QT58SD2OZTHQUGXLSKCAJYZ63NJE2MUIAFQL4KW6PU26YSU786GT0IMCWWKCN25Y7KU0VLU0PPKWR8G6DKWI9BANPIE9I2ZZ5XDUX0TBAY4XFRFQZF087WS9ESTAKIV5V9HSZ2VXW7J5JMGPP4CGYA51BK68T57NN4KTKJVXIQMFXBTN5E3LGKKX3LITQ4C7OPFJ|8|6|5|5|||2|2|1"
		elif lar_file_config["calendar_quarter"]["value"] == 3:
			values_str = f"2|{lei}|{uli}|{year}0113|3|2|2|2|3|218910|5|{year}0808|1234 Hocus Potato Way|Washington|DC|14755|{county}|{censusTract}|1|13||11||KE0NW|1||||||2|2|7||||||||27|24|41|43|2||||3|2|2|2|1|1|75|44|85|0|NA|3|2|8888|8888|9||9||10|||||NA|NA|NA|NA|NA|NA|NA|NA|NA|256|29|2|2|2|2|NA|1|2|4|NA|2|2|NA|3|1|5|1||DOREBESQSW1QT58SD2OZTHQUGXLSKCAJYZ63NJE2MUIAFQL4KW6PU26YSU786GT0IMCWWKCN25Y7KU0VLU0PPKWR8G6DKWI9BANPIE9I2ZZ5XDUX0TBAY4XFRFQZF087WS9ESTAKIV5V9HSZ2VXW7J5JMGPP4CGYA51BK68T57NN4KTKJVXIQMFXBTN5E3LGKKX3LITQ4C7OPFJ|8|6|5|5|||2|2|1"
		elif lar_file_config["calendar_quarter"]["value"] == 2:
			values_str = f"2|{lei}|{uli}|{year}0113|3|2|2|2|3|218910|5|{year}0606|1234 Hocus Potato Way|Washington|DC|14755|{county}|{censusTract}|1|13||11||KE0NW|1||||||2|2|7||||||||27|24|41|43|2||||3|2|2|2|1|1|75|44|85|0|NA|3|2|8888|8888|9||9||10|||||NA|NA|NA|NA|NA|NA|NA|NA|NA|256|29|2|2|2|2|NA|1|2|4|NA|2|2|NA|3|1|5|1||DOREBESQSW1QT58SD2OZTHQUGXLSKCAJYZ63NJE2MUIAFQL4KW6PU26YSU786GT0IMCWWKCN25Y7KU0VLU0PPKWR8G6DKWI9BANPIE9I2ZZ5XDUX0TBAY4XFRFQZF087WS9ESTAKIV5V9HSZ2VXW7J5JMGPP4CGYA51BK68T57NN4KTKJVXIQMFXBTN5E3LGKKX3LITQ4C7OPFJ|8|6|5|5|||2|2|1"
		elif lar_file_config["calendar_quarter"]["value"] == 1:
			values_str = f"2|{lei}|{uli}|{year}0113|3|2|2|2|3|218910|5|{year}0202|1234 Hocus Potato Way|Washington|DC|14755|{county}|{censusTract}|1|13||11||KE0NW|1||||||2|2|7||||||||27|24|41|43|2||||3|2|2|2|1|1|75|44|85|0|NA|3|2|8888|8888|9||9||10|||||NA|NA|NA|NA|NA|NA|NA|NA|NA|256|29|2|2|2|2|NA|1|2|4|NA|2|2|NA|3|1|5|1||DOREBESQSW1QT58SD2OZTHQUGXLSKCAJYZ63NJE2MUIAFQL4KW6PU26YSU786GT0IMCWWKCN25Y7KU0VLU0PPKWR8G6DKWI9BANPIE9I2ZZ5XDUX0TBAY4XFRFQZF087WS9ESTAKIV5V9HSZ2VXW7J5JMGPP4CGYA51BK68T57NN4KTKJVXIQMFXBTN5E3LGKKX3LITQ4C7OPFJ|8|6|5|5|||2|2|1"

		values = values_str.split(sep= "|")
		valid_lar_row = OrderedDict(zip(lar_schema_col_names, values))
  
		return valid_lar_row