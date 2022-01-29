from audioop import avg
from posixpath import split
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRFlights(MRJob):

# Find average distance of flights

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer)
        ]

    def mapper(self, _, line):
        # key is separated with values by tab
        year, items = line.split("\t")

        # removing quotes from strings
        year = year[1:-1]
        items = items[1:-1]

        # unpacking values - 4 columns, only 1 needed
        month, day, airline, distance = items.split(", ")
        distance = int(distance)

        yield month, distance

    def reducer(self, key, values):
        # there is no average function in python to use...
        total = 0
        num_elements = 0

        for value in values:
            total += value
            num_elements += 1

        yield key, total / num_elements

if __name__ == "__main__":
    MRFlights.run()