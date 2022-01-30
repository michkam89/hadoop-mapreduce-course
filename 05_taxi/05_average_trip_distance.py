from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTaxi(MRJob):

    def steps(self):
        return [ 
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
                )
        ]
    
    def mapper(self, _, line):
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
        trip_distance, pickup_longitude, pickup_latitude, RatecodeID, 
        store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
        fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
        improvement_surcharge, total_amount) = line.split(",")


        yield None, float(trip_distance)

    def reducer(self, key, values):
        total_distance = 0
        num_trips = 0
        for value in values:
            total_distance += value
            num_trips += 1

        yield key, total_distance / num_trips


if __name__ == "__main__":
    MRTaxi.run()