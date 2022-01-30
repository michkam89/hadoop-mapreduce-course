from mrjob.job import MRJob
from mrjob.step import MRStep
import re

TIME_RE = re.compile(r'\d\d:\d\d:\d\d')

DECIMALS = 2

class MRTaxi(MRJob):

    def steps(self):
        return [ 
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
                ),
            MRStep(
                mapper=self.mapper_get_keys,
                reducer=self.reducer_get_sorted
            )
        ]
    
    def mapper(self, _, line):
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
        trip_distance, pickup_longitude, pickup_latitude, RatecodeID, 
        store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
        fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
        improvement_surcharge, total_amount) = line.split(",")

        PICKUP_HOUR = TIME_RE.findall(tpep_pickup_datetime).pop(0)[:2]
        # opcjonalnie tpep_pickup_datetime.split()[1][:2]

        yield PICKUP_HOUR, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    MRTaxi.run()