from mrjob.job import MRJob
from mrjob.step import MRStep

class MRFlights(MRJob):

    def mapper(self, _, line):
        # assign names to values
        # column names were removed from .csv
        # will work only if number of elements in line and colnames is equal
        (YEAR,MONTH,DAY,DAY_OF_WEEK,AIRLINE,FLIGHT_NUMBER,TAIL_NUMBER,
        ORIGIN_AIRPORT,DESTINATION_AIRPORT,SCHEDULED_DEPARTURE,DEPARTURE_TIME,
        DEPARTURE_DELAY,TAXI_OUT,WHEELS_OFF,SCHEDULED_TIME,ELAPSED_TIME,AIR_TIME,
        DISTANCE,WHEELS_ON,TAXI_IN,SCHEDULED_ARRIVAL,ARRIVAL_TIME,ARRIVAL_DELAY,
        DIVERTED,CANCELLED,CANCELLATION_REASON,AIR_SYSTEM_DELAY,SECURITY_DELAY,
        AIRLINE_DELAY,LATE_AIRCRAFT_DELAY,WEATHER_DELAY) = line.split(",")

        yield 'row', 1
    
    def reducer(self, key, values):
        yield key, sum(values)
        
if __name__ == "__main__":
    MRFlights.run()