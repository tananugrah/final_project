from numpy import int0
from mrjob.job import MRJob
from mrjob.step import MRStep

class Customer(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort)
        ]

    def mapper(self, _, line):
        (id_transaction,id_customer,birthdate_customer,\
            gender_customer,country_customer,date_transaction,\
                product_transaction,amount_transaction)=line.split(',')
        yield product_transaction,1

    def reducer(self, key, values):
        yield None, (key,sum(values))
    
    def sort(self, key, values):
        data = []
        for product_transaction, order_count in values:
            data.append((product_transaction, order_count))
            data.sort()

        for product_transaction, order_count in data:
           yield product_transaction, order_count


if __name__ == '__main__':
    Customer.run()