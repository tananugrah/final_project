#!python3

from mrjob.job import MRJob
from mrjob.step import MRStep

import csv
import json

cols = 'id_transaction,id_customer,birthdate_customer,gender_customer,country_customer,date_transaction,product_transaction,amount_transaction'.split(',')

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class OrderDateCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort)
        ]

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, csv_readline(line)))

        #skip first row as header
        if row['id_customer'] != 'id_customer':
            # Yield the order_date
            yield row['date_transaction'][0:7],1

    def reducer(self, key, values):
        #for 'order_date' compute
        yield None, (key,sum(values))
    
    def sort(self, key, values):
        data = []
        for date_transaction, order_count in values:
            data.append((date_transaction, order_count))
            data.sort()

        for date_transaction, order_count in data:
           yield date_transaction, order_count

if __name__ == '__main__':
    OrderDateCount.run()