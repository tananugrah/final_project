#!python3

from mrjob.job import MRJob
from mrjob.step import MRStep

import csv
import json


def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class Ordercount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
        ]

    def mapper(self, _, line):
        # Convert each line into a dictionary
        (id_transaction,id_customer,birthdate_customer,\
            gender_customer,country_customer,date_transaction,\
                product_transaction,amount_transaction)=line.split(',')
        if id_customer != 'id_customer':
            yield date_transaction[0:7],int(id_transaction)

    def reducer(self, key, values):
        #for 'order_date' compute
        total = 0
        for x in values:
            total +=1
        yield key,total
    
    
    # def sort(self, key, values):
    #     data = []
    #     for date_transaction, order_count in values:
    #         data.append((date_transaction, order_count))
    #         data.sort()

    #     for date_transaction, order_count in data:
    #        yield date_transaction, order_count

if __name__ == '__main__':
    Ordercount.run()