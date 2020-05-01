import json
import os
import csv
from datetime import datetime

# Curency dimension variables
currencyId = 0
currency_name = str()
currency_dimension_data = dict()
dimension_currency = '/Users/alejandrogandara/Documents/dimension_currencies_export.csv'

# Date dimension variables
dateId = 0
date_dimension_data = dict()
dimension_date = '/Users/alejandrogandara/Documents/dimension_date_export.csv'

# Fact pricing history variables
transactionID = 0
fact_table_pricing_history = '/Users/alejandrogandara/Documents/fact_table_pricing_history.csv'

datasource_directory = '/Users/alejandrogandara/Downloads/Json/'
full_datawarehouse = '/Users/alejandrogandara/Documents/full_datawarehouse.csv'

# Return a set of json files from the data source directory. We need the same order
def get_list_of_json_files():
    json_files = list() # Order is important, so lists is better choice than set
    for filename in os.listdir(datasource_directory):
        if filename.endswith("json"):
            json_files.append(filename)
    return json_files

# We need to open a few csv files at different points of the code.
# First CSV to generate the dimension_currency table. Id and CurrencyName
with open(dimension_currency, 'w', newline='\n') as currencies_export_csv:
    fieldnames = ['CurrencyName', 'CurrencyID']
    write_currency_export = csv.DictWriter(currencies_export_csv, fieldnames=fieldnames)

    json_files = get_list_of_json_files()
    for file in json_files:
        currencyId+=1
        row = {
            'CurrencyName': file.split('.', 2)[0],
            'CurrencyID': currencyId
        }
        # Save row in the csv
        write_currency_export.writerow(row)
        # Add data into a dictionary. Currency name is the key cause we will need to find the ID later on and
        # we will only know the name of the currency
        currency_dimension_data[file.split('.', 2)[0]] = currencyId

# Second CSV for the main report used during the assignment to generate ERD and analyse the data
with open(full_datawarehouse, 'w', newline='\n') as full_export_csv:
    fieldnames = ['currency_name','timestamp','date','price_usd']
    write_full_datawarehouse = csv.DictWriter(full_export_csv, fieldnames=fieldnames)
    write_full_datawarehouse.writeheader()

    # Third CSV for the dimension date
    with open(dimension_date, 'w', newline='\n') as date_export_csv:
        fieldnames = ['DateId', 'DateDay', 'DateMonth', 'DateYear']
        write_date_export = csv.DictWriter(date_export_csv, fieldnames=fieldnames)

        # Forth CSV file for the fact table
        with open(fact_table_pricing_history, 'w', newline='\n') as pricing_history_export_csv:
            fieldnames = ['TransactionId', 'DateId', 'CurrencyId', 'PriceUSD']
            write_pricing_history_export = csv.DictWriter(pricing_history_export_csv, fieldnames=fieldnames)

            json_files = get_list_of_json_files()
            for file in json_files:
                currency_name = file.split('.',2)[0]
                currencyId = currency_dimension_data[currency_name]

                with open(datasource_directory + file) as f:
                    data = json.load(f)
                    # We're inside the json file now. We need to know the size of the lists to know how far to loop
                    # I picked up price_usd, but any other list field will work too
                    for i in range(len(data['price_usd'])):
                        # Every field in this json file has this format [[timestamp,value],[timestamp,value]]
                        timestamp = data['price_usd'][i][0]
                        # We make sure that what we have is a timestamp. Data can be tricky sometimes and lists are
                        # mutable so better be saf
                        if (len(str(timestamp)) > 5) and (str(timestamp)[-3:] == '000'):
                            # Timestamp is in ms, we don't need the extra 000
                            dateRaw = datetime.fromtimestamp(timestamp/1000)
                            # We want the full date for the full report
                            date = int(str(dateRaw.year) + str(dateRaw.month)  + str(dateRaw.day))
                            # Let's prepare our data set, We use set cause we don't want to repeat the object
                            if date not in date_dimension_data.keys():
                                dateId+=1
                                date_dimension_data[date] = dateId
                                # We write our unique date into the csv
                                row_date = {
                                    'DateId': dateId,
                                    'DateDay': int(str(dateRaw.day)),
                                    'DateMonth': int(str(dateRaw.month)),
                                    'DateYear': int(str(dateRaw.year))
                                }
                                write_date_export.writerow(row_date)

                            # We write our unique transaction in the fact table
                            transactionID+=1
                            pricing_history_row = {
                                'TransactionId': transactionID,
                                'CurrencyId': currency_dimension_data[currency_name],
                                'DateId': date_dimension_data[date],
                                'PriceUSD': data['price_usd'][i][1]
                            }
                            write_pricing_history_export.writerow(pricing_history_row)
                            # We write the transaction plain in our full csv
                            full_export_row = {
                                'currency_name': currency_name,
                                'timestamp' : timestamp,
                                'date': date,
                                'price_usd': data['price_usd'][i][1]
                            }
                            write_full_datawarehouse.writerow(full_export_row)
