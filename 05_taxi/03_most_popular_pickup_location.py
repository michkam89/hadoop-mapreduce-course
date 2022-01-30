from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTaxi(MRJob):

    def steps(self):
        return [ 
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer),
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

        pickup_longitude = round(float(pickup_longitude), 5)
        pickup_latitude = round(float(pickup_latitude), 5)

        yield (pickup_latitude, pickup_longitude), 1

    def reducer(self, key, values):
        yield key, sum(values)

    def mapper_get_keys(self, key, value):
        yield None, (value, key)
    
    def reducer_get_sorted(self, key, values):
        self.results = []
        for value in values:
            self.results.append((key, value))
        
        yield None, sorted(self.results, reverse=True)[:10]


if __name__ == "__main__":
    MRTaxi.run()