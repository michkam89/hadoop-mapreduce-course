from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMap(MRJob):

    def mapper(self, _, line):
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
        trip_distance, pickup_longitude, pickup_latitude, RatecodeID, 
        store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
        fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
        improvement_surcharge, total_amount) = line.split(",")

        yield passenger_count, 1

    def reducer(self, key, values):
        total_count = 0
        for value in values:
            total_count += value
            
        yield key, total_count

if __name__ == "__main__":
    MRMap.run()