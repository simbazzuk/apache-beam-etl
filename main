# coding: utf-8
# Updated to Python 3.X

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from datetime import datetime
import apache_beam as beam

# Default PipelineOptions()
p = beam.Pipeline(options=PipelineOptions())

# Inherit as a Class from beam.DoFn
class Transaction(beam.DoFn):
    """
	"""

    def process(self, element):
        """ """
        date, time, id, item = element.split(',')

        return [{"date": date, "time": time, "id": id, "item": item}]


# Used to print to screen
class Printer(beam.DoFn):
    def process(self, data_item):
        print(f'\n{data_item}')


class GetTotal(beam.DoFn):
    """
	"""

    def process(self, element):
        """ """

        # get the total transactions for one item
        return [(str(element[0]), sum(element[1]))]


class FilteringDoFn(beam.DoFn):
    """Exclude all transactions made before the year 2010"""

    def process(self, element):
        d1 = '2010-10-30'
        dt_obj = datetime.strptime(d1, '%Y-%m-%d')
        d2 = datetime.strftime(dt_obj, '%Y-%m-%d').date()
        d3 = d2.year
        dto = (str(element[0]))
        dt1 = datetime.strftime(dto, '%Y-%m-%d').date()
        dt2 = dt1.year
        if dt2 < d3:
            return element


class FilteringTransFn(beam.DoFn):
    """Find all transactions have a `transaction_amount` greater than 20"""

    def process(self, element):
        d1 = '20'
        d2 = int(d1)
        dto = (str(element[1]))
        dt1 = int(dto)
        if dt1 > d2:
            return element


# Use a ParDo
data_from_source = (p
                    | 'ReadMyFile' >> ReadFromText('input/Transactions.csv')
                    )

# Filter by Year
count_of_daily_year = (data_from_source
                       | 'Clean the item year' >> beam.ParDo(Transaction())
                       | 'Map the item to its year' >> beam.Map(lambda record: (record['date'], 1))
                       | 'Filter by Year' >> beam.ParDo(FilteringDoFn())
                       | 'Export results to daily-items-list-year' >> WriteToText('output/daily-items-list-year',
                                                                                  '.txt')

                       )

# Filter by Trans Value
count_of_daily_year = (data_from_source
                       | 'Clean the item value' >> beam.ParDo(Transaction())
                       | 'Map the item to its value' >> beam.Map(lambda record: (record['date'], 1))
                       | 'Filter by Value' >> beam.ParDo(FilteringTransFn())
                       | 'Export results to daily-items-list-trans' >> WriteToText('output/daily-items-list-year',
                                                                                   '.txt')

                       )

# Use the ParDo object
count_of_daily_transactions = (data_from_source
                               | 'Clean the item 01' >> beam.ParDo(Transaction())
                               | 'Map record to 1' >> beam.Map(lambda record: (record['date'], 1))
                               | 'GroupBy the data' >> beam.GroupByKey()
                               | 'Get the total in each day' >> beam.ParDo(GetTotal())
                               | 'Export results to day-list-count file' >> WriteToText('output/day-list-count', '.csv')

                               )

number_of_transactions_per_item = (
        data_from_source
        | 'Clean the item for items count' >> beam.ParDo(Transaction())
        | 'Map record item to 1 for items count' >> beam.Map(lambda record: (record['item'], 1))
        | 'Get the total for each item' >> beam.CombinePerKey(sum)
        | 'Convert data into List' >> (
            beam.CombineGlobally(beam.combiners.ToListCombineFn())  # ToDictCombineFn())
        )
)

result = p.run()
