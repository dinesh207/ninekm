from sample import Scrapper

'''
To use scraper, use initialize() function. It fetches the data in data-frame format.
'''
print('Enter item you want to search:')
search = input()
obj = Scrapper(search)

obj.initialize()
obj.tearDown()
print("Task Completed Successfully!")

# def write_to_excel(dataframe):
#     dataframe.to_excel(search + '_data.xlsx', sheet_name='sheet1', index=False)
#     return dataframe


# df = obj.initialize()
# write_to_excel(df)




# Product full description
# Brand Name
# Company name
# Weight
# Sub-Category
# Parent Category
# Family 
# MRP
# SKU
# Barcode 
# Picture

#Sample Data:

#Aashirvaad Multigrains Atta 5 Kg | Aashirvaad | ITC Limited | 5 Kg | Atta | Staples | Food | 245.00 | 8901725121624

