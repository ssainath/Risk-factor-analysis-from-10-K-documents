#1: Extracting 10Ks and uncertainty words from Edgar
import pandas as pd
from io import StringIO
import requests
import re

def remove_html_tags(text):
#logging.info('Removing the html tags in the response')
    clean = re.compile('<.*?>')
    remove_non_breaking_space = re.compile('&nbsp')
    clean_txt = re.sub(clean, '', text)
    clean_txt = re.sub(remove_non_breaking_space, ' ', clean_txt)
    clean_txt = re.sub(r'\n|\t', ' ', clean_txt)
    clean_txt = re.sub(r'[&#]+[0-9]+', ' ', clean_txt)
    return clean_txt
def max_length(array):
    max=0
    match = ""
    for elem in array:
    if(len(elem) > max):
    max = len(elem)
    match = elem
    return match

def fill_dictionary():
    dict = { "uncertain" : 0,
    "unemploy" : 0,
    "risk" : 0,
    "believe" : 0,
    "anticipate" : 0,
    "fluctuate" : 0,
    "indefinite" : 0,
    "likelihood" : 0,
    "unlikely" : 0,
    "likely" : 0,
    "possible": 0,
    "predict" : 0,
    "recession" : 0,
    "pandemic" : 0,
    "total words" : 0}
    return dict
def count_words(item_1a_section):
    counts= fill_dictionary()
    words = item_1a_section.split()
    for word in words:
    counts['total words'] +=1
    if('uncertain' in word.lower()):
    counts['uncertain'] +=1
    elif('unemploy' in word.lower()):
    counts['unemploy'] +=1
    elif('risk' in word.lower()):
    counts['risk'] +=1
    elif('believe' in word.lower()):
    counts['believe'] +=1
    elif('anticipate' in word.lower()):
    counts['anticipate'] +=1
    elif('fluctuate' in word.lower()):
    counts['fluctuate'] +=1
    elif('indefinite' in word.lower()):
    counts['indefinite'] +=1
    elif('likelihood' in word.lower()):
    counts['likelihood'] +=1
    elif('unlikely' in word.lower()):
    counts['unlikely'] +=1
    elif('likely' in word.lower()):
    counts['likely'] +=1
    elif('possible' in word.lower()):
    counts['possible'] +=1
    elif('predict' in word.lower()):
    counts['predict'] +=1
    elif('recession' in word.lower()):
    counts['recession'] +=1
    elif('pandemic' in word.lower()):
    counts['pandemic'] +=1
    return counts
    
#logging.info('Configuring parameters')
years = range(2012,2013)
document= '10-K'
col_specification = [(0, 61), (62, 73), (74, 85), (86, 97), (98, 159)]
columnHeaders = ['company_name','form_type','cik','date','file_name']
dataframe_10k_q = pd.DataFrame(columns = columnHeaders)
dataframe_10k = pd.DataFrame(columns = columnHeaders)
for year in years:
    #logging.debug('for each year..')
    for quarter in ['QTR1', 'QTR2', 'QTR3', 'QTR4']:
    #logging.debug('for each quarter')
        indexfile = r'https://www.sec.gov/Archives/edgar/full-index/' + str(year) + '/' + quarter +
        '/company.idx'
        realindexfile = requests.get(indexfile, allow_redirects=True)
        realindexfile.encoding = 'utf-8'
        dataframe = pd.read_fwf(StringIO(realindexfile.text), colspecs=col_specification,
        skiprows=9)
        #dataframe.to_csv('C:\\Users\\Shreya Sainathan\\Downloads\\file_name.csv')
        #name the columns
        dataframe.columns = columnHeaders
        if(dataframe_10k.empty == True):
            dataframe_10k = dataframe[(dataframe['form_type']==(document))]
        else:
            dataframe_10k =
            dataframe_10k.append(dataframe[(dataframe['form_type']==(document))])
            dataframe_10k.to_csv('10-Ks.csv')
            #logging.debug('getting the 10-k data for each company')
            for i in range(len(dataframe_10k)) :
            url = 'https://www.sec.gov/Archives/' + dataframe_10k.iloc[i]['file_name']
            #print(url)
            response = requests.get(url)
            response.encoding = 'utf-8'
            clean_txt = remove_html_tags(response.text)
            ##item_1 = r"(Item[\s;]*?1A[.]{0,1}).*?(Risk[\s]+?Factors)"
            ##item_1b = r"Item[\s;]*?1B[.]{0,1}.*?Unresolved[\s]+staff[\s]+comments"
            #item_1b = r"Item.*?1B[.]{0,1}.*?Unresolved[\s]+staff[\s]+comments"
            item_all =
            "Item[\s;]*?1A[.]{0,1}.{0,10}Risk[\s]+?Factors.*?Item[\s;]*?1B[.]{0,1}.{0,10}Unresolved[\s]+staff[\s]
            +comments"
            x = re.findall(item_all, clean_txt,flags=re.IGNORECASE|re.DOTALL|re.M)#risk.*factors
            .*item.*1B.*unresolved.*staff.*comments
            counts_dictionary = count_words(max_length(x))
            counts_dataframe = pd.DataFrame(counts_dictionary, index=[0])
            to_write = dataframe_10k.iloc[[i]][['cik', 'company_name', 'date']]
            to_write.index = [0]
            result = pd.concat([to_write, counts_dataframe], axis=1, sort=False)
            result.to_csv('final_data.csv', mode = 'a')
            
            
#Extract Return on Assets Data
from bs4 import BeautifulSoup
import requests
import re
#response object
company = open(r"returnonassets_data.txt", "r")
newfile = open("moredata2.txt", "w+")
data_and_url = []
line = company.readline()
linenumber = 0
while (line != ""):
    line = company.readline()
    column = re.split(r'\s{2,}', line)
    if (len(column)>7):
    if (linenumber!=0):
    if (column[6] != ".") and (column[5] != ".") and (column[6] != "C") and (column[5] != "C"):
        newval = float(column[6])/float(column[5])
        newfile.write(line[:-1] + " " + str(newval) + "\n")
    else:
        newval = "."
        newfile.write(line[:-1] + " " + newval + "\n")
    else:
        linenumber += 1
        newfile.write(line[:-1] + " roa/n")
        company.close()
        newfile.close()
        
        
#3: Extract Return on Stock Data
from bs4 import BeautifulSoup
import requests
import re
#response object
company = open(r"annual_prices.txt", "r")
newfile = open("stockreturn_final.txt", "w+")
data_and_url = []
line = company.readline()
linenumber = 0
listofcompanies = []
previousline=[]
while (line != ""):
    line = company.readline()
    column = re.split(r'\s{2,}', line)
    if (len(column)>7):
    if (linenumber!=0):
    if (column[6] != ".") and (column[7] != ".") and (column[6] != "C") and (column[7] != "C"):
        if column[4] not in listofcompanies:
            listofcompanies.append(column[4])
            newval = "-"
            newfile.write(line[:-1] + " " + str(newval) + "\n")
            previousline=[]
            previousline.append(column[7])
        else:
            element=previousline[0]
            p1 = float(element)
        if float(column[7]) != 0:
            newval = (((p1-float(column[7]))+float(column[6]))/float(column[7])) *100
            newfile.write(line[:-1] + " " + str(newval) + "\n")
            previousline=[]
            previousline.append(column[7])
        else:
            newval = "n/a"
            newfile.write(line[:-1] + " " + str(newval) + "\n")
            previousline=[]
            previousline.append(column[7])
        else:
            linenumber += 1
            newfile.write(line[:-1] + " Stock Return" + "\n")
            company.close()
            newfile.close()
            
            
#4: Apply Major Industry to Data
from bs4 import BeautifulSoup
import requests
import re
#response object
company = open(r"stockreturn_final.txt", "r")
newfile = open("stockreturn_industry.txt", "w+")
line = company.readline()
linenumber = 1
while (line != ""):
    line = company.readline()
    column = re.split(r'\s{2,}', line)
    if (len(column)>7):
        if (linenumber!=1):
            siccolumn = column[3]
            sic = siccolumn[:2]
            if sic in ("01", "02", "03", "04","05", "06", "07", "08", "09"):
                industry = "Agriculture, Forestry, And Fishing"
                newfile.write(line[:-1] + " " + industry + "\n")
            elif sic in ("10", "11", "12", "13", "14"):
                industry = "Mining"
                newfile.write(line[:-1] + " " + industry + "\n")
            elif sic in ("15", "16", "17"):
                industry = "Construction"
                newfile.write(line[:-1] + " " + industry + "\n")
            elif sic in ("20", "21", "22", "23","24", "25", "26", "27", "28","29", "30", "31", "32","33", "34",
            "35", "36", "37", "38", "39"):
                industry = "Manufacturing"
                newfile.write(line[:-1] + " " + industry + "\n")
            elif sic in ("40", "41", "42", "43","44", "45", "46", "47", "48", "49"):
                industry = "Transportation, Communications, Electric, Gas, Sanitation"
                newfile.write(line[:-1] + " " + industry + "\n")
            elif sic in ("50", "51"):
                industry = "Wholesale Trade"
                newfile.write(line[:-1] + " " + industry + "\n")
            elif sic in ("52", "53", "54", "55","56", "57", "58", "59", "60"):
                industry = "Retail Trade"
                newfile.write(line[:-1] + " " + industry + "\n")
            elif sic in ("60", "61", "62", "63","64", "65", "66", "67"):
                industry = "Finance, Insurance, and Real Estate"
                newfile.write(line[:-1] + " " + industry + "\n")
            elif sic in ("70", "71", "72", "73","74", "75", "76", "77", "78","79", "80", "81", "82","83", "84",
            "85", "86", "87", "88", "89"):
                industry = "Services"
                newfile.write(line[:-1] + " " + industry + "\n")
            elif sic in ("90", "91", "92", "93","94", "95", "96", "97", "98", "99"):
                industry = "Public Administration"
                newfile.write(line[:-1] + " " + industry + "\n")
            else:
                industry = " "
                newfile.write(line[:-1] + " " + industry + "\n")
    linenumber += 1
    newfile.write(line + " Industry\n")
    company.close()
    newfile.close()
    
    
#5: Use matplotlib to create graphs out of our data
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import linregress
#uses any merged csv
df = pd.read_csv("2015-2019 final merged.csv", dtype={"cik": pd.Int64Dtype(), "total words":
pd.Int64Dtype(), 'roa': float})
#make columns lists of values (remember to change the column into floats with dtype ^)
roa = df['roa'].tolist()
uncertain = df['uncertain'].tolist()
totalwords = df['total words'].tolist()
'''
#you can do this to any two cols in the data
plt.scatter(uncertain, roa, s=1)
uncertainaxes = plt.gca()
uncertainaxes.set_ylim([-10,10])
plt.ylabel('return on assets')
plt.xlabel('number of mentions of word "uncertain"')
plt.title('return on assets vs metions of "uncertain"')
plt.grid()
plt.show()
'''
#scatter plot, s is size
plt.scatter(totalwords, roa, s=1)
#label axis
plt.ylabel('return on assets')
plt.xlabel('word count of section 1A risk factors')
plt.title('return on assets vs total word count')
#show grid lines
plt.grid()
wordsaxes = plt.gca()
#set x,y axis limits
wordsaxes.set_xlim([0,80000])
wordsaxes.set_ylim([-10,10])
plt.show()
#if you want to save the graph just click the save window shown


#6: Combine the variables from CRSP/Compustat and the word count data
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import numpy as np
from io import StringIO
import requests
import re
df = pd.read_csv("2007copy.csv", dtype={"cik": pd.Int64Dtype()}, skiprows=lambda x: (x != 0)
and not x % 2)
companydata = pd.read_csv("wrds data.csv", dtype={"fyear": pd.Int64Dtype() ,"cik":
pd.Int64Dtype()}, usecols=["conm", "cik","fyear", "at", "ni", "roa"], na_values = ".")
companydata = companydata[['cik', 'conm', 'fyear', "at", "ni", "roa"]]
companydata.to_csv("betterroa.csv", index = False)
final = df.copy()
final.reset_index(drop=True)
final["at"] = ""
final["ni"] = ""
final["roa"] = ""
del final['Unnamed: 0']
linenumber = 0
list_of_lists = []
for index, row in final.iterrows():
    cik = row.loc['cik']
    date = row.loc['date']
    year = int(date[:4])
    wordnumber = row.loc['total words']
    if (wordnumber<100):
        final.drop([index], axis=0, inplace=True)
        continue
    
    found = companydata.loc[companydata['cik'] == cik]
    if not (found.empty):
        #now match year
        foundyear = found.loc[found['fyear'] == year]
    if not (foundyear.empty):
        '''
        if (foundyear.iloc[0]['at'] == ""):
        final.drop([index], axis=0, inplace=True)
        continue
        '''
        final.loc[index, 'at'] = foundyear.iloc[0]['at']
        final.loc[index, 'ni'] = foundyear.iloc[0]['ni']
        final.loc[index, 'roa'] = foundyear.iloc[0]['roa']
    else:
        final.drop([index], axis=0, inplace=True)
    else:
        final.drop([index], axis=0, inplace=True)
        final.to_csv("merged data.csv")
