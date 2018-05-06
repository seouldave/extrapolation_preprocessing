"""Module which defines classes and functions to open 2 rasters of same extent.
One raster defines zones, the other is binary. Functions will open both rasters
and put them into individual NumPy arrays. It will create a table summing the binary
pixels where they are inside zonal pixels, creating a summary of pixels per zone.

Parameters:
ISO

Methods (see individual methods for more info)
__init___ ---------------> Make Zonal stats object
make_folders ------------>Prepare folders to download data [datain/<iso>/urban; ppp 
download_pop_table ------>Download 2000-2020 population tables [datain/<iso>/]
download_adm ------------>Download ccidL1 admin units raster [datain/<iso>]
download_urban ---------->Download binary urban rasters 2001-2014 -> datain/<iso>/urban/
download_ppp ------------>Download population rasters 2001-2014 -> datain/<iso>/ppp/
calc_stats -------------->Calculate count of urban pixels and population per L1 unit
concat_dataframes ------->Join all dataframes for each year to 1 long dataframe
delete_files ------------>Clean system of downloaded data (keep only output tables)
"""
from osgeo import gdal
import pandas as pd 
import numpy as np 
import time
import os
from collections import Counter
from ftplib import FTP
from dev_settings import *
import scipy.stats 

class Zonal_stats:
	"""Class of download, open and analyse rasters and calculate zonal stats"""
	def __init__(self, iso):
		self.iso = iso
		self.datain = self.make_folders(self.iso)
		self.pop_table = self.download_pop_table(self.iso)
		self.adm = self.download_adm(self.iso)
		self.download_urban_data(self.iso)
		self.download_ppp_data(self.iso)
		self.dataframes_to_concat = []

	def make_folders(self, iso):
		"""Method to prepare folders where data is downloaded
		
		Parameters:
		iso

		Returns:
		None
		"""
		datain = 'datain/{0}'.format(iso)
		if not os.path.exists(datain):
			os.makedirs(datain)
			os.makedirs(os.path.join(datain, "ppp"))
			os.makedirs(os.path.join(datain, "urban"))
		return datain

	def download_pop_table(self, iso):
		"""Method to download pop tables from FTP

		Parameters:
		iso

		Returns:
		pop_table_path
		"""
		filename = "{0}_population_2000_2020.csv".format(iso.lower())
		ftp = FTP(FTP_url)
		ftp.login(FTP_username, FTP_password)
		ftp.cwd('WP515640_Global/CensusTables')
		lf = open('datain/{0}/{1}'.format(iso, filename), 'wb')
		ftp.retrbinary('RETR ' + filename, lf.write)
		lf.close()
		pop_table = pd.read_csv("datain/{0}/{1}".format(iso, filename), usecols = \
			range(1,17))
		return pop_table

	def download_ftp(self, filename, ftp_path, download_path):
		"""Method to download data from location in FTP to location in filesystem

		Parameters:
		filename ---------->Name of target and download
		ftp_path ---------->Location of target
		download_path------>Location of download
		"""		
		ftp = FTP(FTP_url)
		ftp.login(FTP_username, FTP_password)
		ftp.cwd(ftp_path)
		lf = open(download_path, 'wb')
		ftp.retrbinary('RETR ' + filename, lf.write)
		lf.close()
		ftp.quit()

	def download_adm(self, iso):
		"""Method to download adminL1 raster for country. Same raster used for
		each year

		Parameters:
		iso

		Returns:
		path to raster
		"""
		filename = "{0}_grid_100m_ccidadminl1.tif".format(iso.lower())
		ftp_path = 'WP515640_Global/Covariates/{0}/Mastergrid'.format(iso)
		download_path = 'datain/{0}/{1}'.format(iso, filename)
		self.download_ftp(filename, ftp_path, download_path)
		return download_path

	def download_urban_data(self, iso):
		"""Method to download binary urban data from FTP from 2001-2014

		Parameters:
		iso

		Returns:
		None
		"""

		#for year in range(2000, 2015, 1):
		#	ftp_path = "/WP515640_Global/Covariates/{0}/BuiltSettlement/{1}/Binary".format(iso, year)
		for year in [2000, 2012, 2014]:
			ftp_path = "/WP515640_Global/Covariates/{0}/BuiltSettlement/{1}/Binary".format(iso, year)
			if year == 2000:
				filename = "{0}_grid_100m_ghsl_esa_{1}.tif".format(iso.lower(), year)
				download_path = 'datain/{0}/urban/{1}'.format(iso, filename)
				self.download_ftp(filename, ftp_path, download_path)
			elif year == 2012:
				filename = "{0}_grid_100m_ghsl_guf_{1}.tif".format(iso.lower(), year)
				download_path = 'datain/{0}/urban/{1}'.format(iso, filename)
				self.download_ftp(filename, ftp_path, download_path)
			elif year == 2014:
				filename = "{0}_grid_100m_guf_ghsl_{1}.tif".format(iso.lower(), year)
				download_path = 'datain/{0}/urban/{1}'.format(iso, filename)
				self.download_ftp(filename, ftp_path, download_path)
			else:
				filename = "BSGM_Extentsprj_2000-2012_{0}_{1}.tif".format(iso, year)
				download_path = 'datain/{0}/urban/{1}'.format(iso, filename)
				self.download_ftp(filename, ftp_path, download_path)
			print(filename)


		


	def download_ppp_data(self, iso):
		"""Method to download PPP population data from FTP from 2001-2014

		Parameters:
		iso

		Returns:
		None
		"""
		#for year in range(2000, 2015, 1):
		#	ftp_path = "/WP515640_Global/ppp_datasets/{0}/{1}".format(year, iso)
		for year in [2000, 2012, 2014]:
			ftp_path = "/WP515640_Global/ppp_datasets/{0}/{1}".format(year, iso)
			filename = "{0}_ppp_wpgp_{1}.tif".format(iso.lower(), year)
			download_path = 'datain/{0}/ppp/{1}'.format(iso, filename)
			self.download_ftp(filename, ftp_path, download_path)


	def get_bins(self, data, nodata):
		"""Method to return bins of unique admin unit codes and counts for zonal
		stats table

		Parameters:
		data ---------> np.array (flattened)
		nodata -------> Nodata value of the raster (these values need to be ignored)
		"""
		bins = np.unique(data)
		bins = np.delete(bins, np.where(bins == nodata))
		if len(bins) == 0:
			bins = np.empty(1,)
		return np.append(bins, max(bins) + 1)


	def calc_stats(self):
		"""Method to calculate zonal statistics of urban pixels per adm_unit (count)
		and population in urban pixes per adm_unit (sum) for each year. Dataframe of 
		stats created for each year created, and then all dataframes are merged together

		Parameters:
		iso

		Returns
		None
		"""
		#for year in range(2000, 2015, 1):
		iso = self.iso
		for year in [2000, 2012, 2014]:
			out_fn = 'datain/{0}/{0}_BS_PIX_POP_{1}.csv'.format(iso, year)
			urban_pop = 'datain/{0}/ppp/{1}_ppp_wpgp_{2}.tif'.format(iso, iso.lower(), year)
			adm = self.adm
			if year == 2000:
				urban_mask = "datain/{0}/urban/{1}_grid_100m_ghsl_esa_{2}.tif".format(iso, iso.lower(), year)
			elif year == 2012:
				urban_mask = "datain/{0}/urban/{1}_grid_100m_ghsl_guf_{2}.tif".format(iso, iso.lower(), year)
			elif year == 2014:
				urban_mask = "datain/{0}/urban/{1}_grid_100m_guf_ghsl_{2}.tif".format(iso, iso.lower(), year)
			else:
				urban_mask = "datain/{0}/urban/BSGM_Extentsprj_2000-2012_{0}_{1}.tif".format(iso, year)


			pop_ds = gdal.Open(urban_pop)
			pop_band = pop_ds.GetRasterBand(1)
			num_cols = pop_band.XSize
			num_rows = pop_band.YSize
			pop_nd = pop_band.GetNoDataValue()
			pop_block_xsize, pop_block_ysize = pop_band.GetBlockSize()

			adm_ds = gdal.Open(adm)
			adm_band = adm_ds.GetRasterBand(1)
			adm_nd = adm_band.GetNoDataValue()
			adm_block_xsize, adm_block_ysize = adm_band.GetBlockSize()

			urban_ds = gdal.Open(urban_mask)
			urban_band = urban_ds.GetRasterBand(1)

			list_df = [] #list to put sums of population per admin unit bin in each block
			stats = [] #list to put counts of urban pixels per admin unit


			for x in range(0, num_cols, pop_block_xsize):
						if x + pop_block_xsize < num_cols:
							cols = pop_block_xsize
						else:
							cols = num_cols - x
						for y in range(0, num_rows, pop_block_ysize):
							#if y%10000 == 0:
							if y + pop_block_ysize < num_rows:
								rows = pop_block_ysize
							else:
								rows = num_rows - y
							
							urban_data = urban_band.ReadAsArray(x, y, cols, rows) #Read urban to mask population outside of urban pixels (see below)

							pop_data = pop_band.ReadAsArray(x, y, cols, rows)
							pop_data[np.where(urban_data == 0)] = 0 #Mask out population not in urban pixels
							pop_data = pop_data.flatten()
							pop_bins = self.get_bins(pop_data, pop_nd)

							adm_data_2d = adm_band.ReadAsArray(x, y, cols, rows) #Keep on array 2D for urban pixel counts
							adm_data = adm_data_2d.flatten()
							adm_bins = self.get_bins(adm_data, adm_nd)

							hist, pop_bins2, adm_bins2, bn = \
							scipy.stats.binned_statistic_2d(adm_data, pop_data, pop_data, 'sum', [adm_bins, pop_bins]) #returns numpy array representing histogram (rows = admin_units; cols = unique pixel values)
							hist = np.insert(hist, 0 , pop_bins[:-1], 0) # column labels added
							row_labels = np.insert(adm_bins[:-1], 0, 0) # row lables added
							hist = np.insert(hist, 0, row_labels, 1) # row lables added
							hist = np.column_stack((hist[:,:1], np.sum(hist[:,1:], axis=1)))[1:].tolist() #columns labels removed and all columns of counts summed and joined to first column (admin codes)
							#df = pd.DataFrame(hist)

							counts = Counter(adm_data_2d[np.where(urban_data == 1)].tolist()) #Counts of urban pixels per admin_unit 

							#counts appened to stats list 
							for i in counts:
								stats.append([i, counts[i]])

							#pop historgrams appended to list_df
							for i in hist:
								if i[0] > 0:
									list_df.append(i)

			df = pd.DataFrame(list_df, columns=['GID', 'BSPOP']).set_index('GID').groupby('GID').sum() #Dataframe of sum of population per admin_id
			df_counts = pd.DataFrame(stats, columns=['GID', 'BSCNT']).set_index('GID').groupby('GID').sum() #Dataframe of count of urban pixels per admin_id
			df = pd.concat([df, df_counts.iloc[np.where(df_counts.index != 0)]], axis=1) #Join dataframes ignoring water counts from the L1 raster
			df['BSCNT'] = np.where(df['BSCNT'].isnull(), 0, df['BSCNT']) #Where no urban pixels in admin unit, these values will be NULL in dataframe -> converted to 0
			df.to_csv(out_fn)



