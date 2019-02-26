import pandas as pd
import os


class DataProcessor:
    def __init__(self):
        self._ = None
        self.data_dir = '../data/'

    def _read_data(self, folder, fname):
        fpath = self.data_dir + folder + fname
        data = pd.read_csv(fpath)

        return data

    def _merge_data(self):
        trip_data = self._read_data(fname='yellow_tripdata_2017-06.csv', folder='raw/')
        zone_lkp = self._read_data(fname='taxi+_zone_lookup.csv', folder='raw/')

        pu_zone_lkp = zone_lkp.add_prefix('PU')
        do_zone_lkp = zone_lkp.add_prefix('DO')

        pu_zone_lkp['PULocationID'] = pu_zone_lkp['PULocationID']

        # Mapping pick up location id
        data = trip_data.merge(pu_zone_lkp, how='left', on='PULocationID', validate='m:1')
        # Mapping drop off location id
        data = data.merge(do_zone_lkp, how='left', on='DOLocationID', validate='m:1')

        data.to_csv(self.data_dir + 'interim/merged_raw_data.csv', index=False)

        return data

    def make_yellow_taxi_data(self, merged_raw_data_fname='merged_raw_data.csv', folder='interim/'):

        merged_raw_data_path = self.data_dir + folder + merged_raw_data_fname

        if not os.path.isfile(merged_raw_data_path):
            data = self._merge_data()
        else:
            data = pd.read_csv(merged_raw_data_path)

        # tpep_pickup_datetime
        data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
        data['pickup_weekday'] = data['tpep_pickup_datetime'].dt.dayofweek
        data['pickup_month'] = data['tpep_pickup_datetime'].dt.month
        data['pickup_day'] = data['tpep_pickup_datetime'].dt.day
        data['pickup_hr'] = data['tpep_pickup_datetime'].dt.hour

        # tpep_dropoff_datetime
        data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])
        data['dropoff_weekday'] = data['tpep_dropoff_datetime'].dt.dayofweek
        data['dropoff_month'] = data['tpep_dropoff_datetime'].dt.month
        data['dropoff_day'] = data['tpep_dropoff_datetime'].dt.day
        data['dropoff_hr'] = data['tpep_dropoff_datetime'].dt.hour

        data['trip_duration'] = (data['tpep_dropoff_datetime'] - data['tpep_pickup_datetime']) / pd.Timedelta('1 min')

        # Assumption: take-home amount for drivers does not include tax, toll, and improvement surcharges
        data['driver_received_amount'] = data['fare_amount'] + data['tip_amount']

        data.to_csv(self.data_dir + 'processed/yellow_taxi_data.csv', index=False)

        return data


if __name__ == '__main__':
    data_processor = DataProcessor()
    taxi_data = data_processor.make_yellow_taxi_data()
    print(taxi_data.head())
    print(taxi_data.columns.values)

