from multiprocessing import Process, Queue
def get_dates(queue, start_date='2010/07/17', end_date='2023/01/07'):
	from bs4 import BeautifulSoup #module for web scraping install by pip install beautifulsoup4
	import requests #for requesting html. install by pip install requests
	from requests.adapters import HTTPAdapter
	from requests.packages.urllib3.util.retry import Retry
	import re #regular expression for data extraction by pattern matching. installed by default.
	import pandas as pd # for dataframe. install by pip install pandas
	from csv import reader #for list structure. installed by default.
	from tqdm import tqdm
	import os
	import shutil
	import uuid
	import time
	
	start_date = start_date
	end_date = end_date
	
	dir = str(uuid.uuid4())
	if os.path.exists(dir):
	    shutil.rmtree(dir)
	os.makedirs(dir)
	features=pd.DataFrame({'features':['transactions','size','sentbyaddress','difficulty','hashrate','mining_profitability','sentinusd','transactionfees','median_transaction_fee','confirmationtime','transactionvalue','mediantransactionvalue','activeaddresses','top100cap','fee_to_reward','price']})
	indicators=pd.DataFrame({'indicators':['sma','ema','wma','trx','mom','std','var','rsi','roc']})
	periods=pd.DataFrame({'periods':['3','7','14','30','90']})
	crypto=pd.DataFrame({'crypto':['btc']})
	df=pd.concat([crypto, features,indicators,periods], axis=1)

	#for raw values
	#all kinds of fees and transaction values are in USD. divide by price USD to obtain BTC
	url_list=[] #stores generated urls
	feature_list=[] #store feature names
	i=0
	while (i<=15): #this loop generates urls for raw values
		url='https://bitinfocharts.com/comparison/'+df['features'][i]+'-'+'btc'+'.html'
		feature = df['features'][i]
		if "fee" in feature:
			feature=df['features'][i]+'USD'
		if 'value' in feature:
			feature=df['features'][i]+'USD'
		if 'usd' in feature:
			feature=df['features'][i]+'USD'
		url_list.append(url)
		feature_list.append(feature)
		#print(feature,' ',url)
		i=i+1
	#for indicators
	#all kinds of fees and transaction values are in USD. drop them or recalculate them after converting the raw values to the BTC
	i=0
	while (i<=15): #this nested while loop generates url structure for all the indicators. for other currencies change btc to CURRENCY_NAME
		j=0
		while (j<=8):
			k=0
			while (k<=4):
				url='https://bitinfocharts.com/comparison/'+df['features'][i]+'-'+'btc'+'-'+df['indicators'][j]+df['periods'][k]+'.html'
				feature=df['features'][i]+df['periods'][k]+df['indicators'][j]
				if "fee" in feature:
					feature=df['features'][i]+df['periods'][k]+df['indicators'][j]+'USD'
				if 'value' in feature:
					feature=df['features'][i]+df['periods'][k]+df['indicators'][j]+'USD'
				if 'price' in feature:
					feature=df['features'][i]+df['periods'][k]+df['indicators'][j]+'USD'
				if 'usd' in feature:
					feature=df['features'][i]+df['periods'][k]+df['indicators'][j]+'USD'
				if 'fee_in_reward' in feature:
					feature=df['features'][i]+df['periods'][k]+df['indicators'][j]
				url_list.append(url)
				feature_list.append(feature)
				#print(feature,' ',url)
				k=k+1
			j=j+1
		i=i+1
	


	df_feature=pd.DataFrame(feature_list,columns=['Features']) # convert feature list to dataframe
	df_url=pd.DataFrame(url_list,columns=['URL']) # convert url list to dataframe

	df2=df_feature.join(df_url) # join the feature and url dataframes 
	
	features=pd.DataFrame(columns=df2.Features) #change the feature list to columns

	columns=len(features.columns) #to be used in while loop for getting data
	columns

	date=[] #create a date column for each feature. this is necessary for aligning by dates later
	print('Building URLs ...')	
	for i in tqdm(range(len(features.columns))):
	    date=features.columns[i] + 'Date'
	    features[date]=date

	i=0
	print('Requesting data ... ')
	for i in tqdm(range(columns)): #the most important. getting the data from the website. DONT ABUSE IT. you might be IP banned for requesting a lot
	    columnNames=[features.columns[i+columns],features.columns[i]]
		url = df2.URL[i]
		session = requests.Session()
		retry = Retry(connect=10, backoff_factor=3)
		adapter = HTTPAdapter(max_retries=retry)
		session.mount('http://', adapter)
		session.mount('https://', adapter)
		page=session.get(url)
		#page = requests.get(url, time.sleep(3),timeout=10)
		#print(page)
		soup = BeautifulSoup(page.content, 'html.parser')
		values=soup.find_all('script')[4].get_text()
		newval=values.replace('[new Date("','')
		newval2=newval.replace('"),',";")
		newval3=newval2.replace('],',',')
		newval4=newval3.replace('],',']]')
		newval5=newval4.replace('null','0')
		x = re.findall('\\[(.+?)\\]\\]', newval5)
		df3=pd.DataFrame( list(reader(x)))
		df_transposed=df3.transpose()
		df_transposed.columns=['Value']
		df_new=df_transposed['Value'].str.split(';', 1, expand=True)
		df_new.columns= columnNames
		mask = (df_new[features.columns[i+columns]] >= start_date) & (df_new[features.columns[i+columns]] <= end_date)
		df_new= df_new.loc[mask]
		features[features.columns[i]] = df_new[features.columns[i]]
		features[features.columns[i+columns]]= df_new[features.columns[i+columns]]
		df_new.columns = df_new.columns.str.replace('.*Date*', 'Date')
		path=dir+'/'+features.columns[i]+'.csv'
		df_new.set_index('Date', inplace=True)
		df_new.to_csv(path,sep=',', columns=[features.columns[i]])
		#print(df_new)
		#i = i+1

	i=0
	pricepath=dir+'/'+'price.csv'
	df_merge=pd.read_csv(pricepath,sep=',')
	print('Processing files ... ')
	for i in tqdm(range(columns)):
		path=dir+'/'+features.columns[i]+'.csv'
		df=pd.read_csv(path,sep=',')
		df_merge = pd.merge(df_merge, df, left_on='Date', right_on='Date')
		#i = i+1
	
	df_merge.drop(columns=['price_y'], inplace=True)
	df_merge.columns = df_merge.columns.str.replace('price_x', 'priceUSD')

	df_merge
	
	path2=dir+'/'+'Merged_Unconverted_BTC_Data.csv'
	df_merge.to_csv(path2,sep=',')
	df_all = pd.read_csv(path2,sep = ',')
	df_all.columns
	
	#mediantransactionvalue_cols = [col for col in df_all.columns if 'mediantransactionvalue' in col]
	#mediantransactionvalue_BTC_cols = [w.replace('USD', 'BTC') for w in mediantransactionvalue_cols]
	#df_all[mediantransactionvalue_BTC_cols] = df_all[mediantransactionvalue_cols].div(df_all['priceUSD'].values,axis=0)

	#sentinusd_cols = [col for col in df_all.columns if 'sentinusd' in col]
	#sentinusd_BTC_cols = [w.replace('USD', 'BTC') for w in sentinusd_cols]
	#df_all[sentinusd_BTC_cols] = df_all[sentinusd_cols].div(df_all['priceUSD'].values,axis=0)

	#transactionfees_cols = [col for col in df_all.columns if 'transactionfees' in col]
	#transactionfees_BTC_cols = [w.replace('USD', 'BTC') for w in transactionfees_cols]
	#df_all[transactionfees_BTC_cols] = df_all[transactionfees_cols].div(df_all['priceUSD'].values,axis=0)

	#median_transaction_fee_cols = [col for col in df_all.columns if 'median_transaction_fee' in col]
	#median_transaction_fee_BTC_cols = [w.replace('USD', 'BTC') for w in median_transaction_fee_cols]
	#df_all[median_transaction_fee_BTC_cols] = df_all[median_transaction_fee_cols].div(df_all['priceUSD'].values,axis=0)

	#transactionvalue_cols = [col for col in df_all.columns if 'transactionvalue' in col]
	#transactionvalue_BTC_cols = [w.replace('USD', 'BTC') for w in transactionvalue_cols]
	#df_all[transactionvalue_BTC_cols] = df_all[transactionvalue_cols].div(df_all['priceUSD'].values,axis=0)

	#USDvalue_cols = sentinusd_cols+transactionfees_cols+median_transaction_fee_cols+mediantransactionvalue_cols+transactionvalue_cols

	#df_all.drop(USDvalue_cols, axis=1, inplace=True)

	#df_all.drop(list(df_all.filter(regex = 'price')), axis = 1, inplace = True)

	df_all.drop(columns=['Unnamed: 0'], axis=1, inplace=True)

	df_all['priceUSD']=df_merge['priceUSD']
	df_all
	
	filename=dir+'_'+'BTC_Data.csv'
	df_all.to_csv(filename,sep=',')
	
	queue.put(df_all)

def get_data(start_date='2010/07/17', end_date='2023/01/07'):
	'''
	def get_data(start_date, end_date):
		...
		return df

	Note: date format in YYYY/MM/DD	
	
	Example:

	from datacollector import get_data

	df=get_data('2020/01/01','2020/01/07')

	'''
	q = Queue()
	p = Process(target=get_dates, args=(q, start_date, end_date))
	p.start()
	df=q.get()
	p.join()
	return df

if __name__ == '__main__':
	get_data()

