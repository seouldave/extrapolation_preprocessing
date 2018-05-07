import sys
from src.extrapolation_preprocessing import *

def main():
	iso = sys.argv[1]
	zonal_stats = Zonal_stats(iso)
	zonal_stats.calc_stats()
	zonal_stats.concat_dataframes(iso)
	zonal_stats.delete_files(iso)
	


if __name__ == "__main__":
	main()