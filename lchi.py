import pandas as pd
import numpy as np
import os


def save_file(source, main, new, name):
	with open(name_file) as fw:
		common_file = main + new + '\\' + name + '.csv'
		source.to_csv(common_file, header=True, index=False, encoding='utf-8', sep=';')


def load_file(file_name):
	main_dir = os.path.dirname(file_name) + '\\'
	new_dir = file_name.split('\\')[-1][:-4]
	new_name = file_name.split('\\')[-1][2:]
	if not os.path.exists(main_dir + new_dir):
		os.mkdir(main_dir + new_dir)
	source = pd.read_csv(file_name, sep=";", header=None)
	source.columns = ['Time', 'Tiker', 'Quant', 'Price']
	date_time = source['Time'].str.split(' ', expand=True)
	date_time.columns = ['Date', 'Time']
	
	source.insert(0, 'Date', True)
	source['Date'] = date_time['Date']
	source['Time'] = date_time['Time']
	source['Tiker'] = source['Tiker'].str.strip()
	
	time = source['Time'].str.split('.', expand=True)
	time.columns = ['Time', 'Time2']
	source['Time'] = time['Time']
	# save_file(source, main_dir, new_dir, name)
	
	tickers = sorted(source['Tiker'].unique().tolist())
	source = source.groupby(['Date', 'Time', 'Tiker', 'Price'], as_index=False)['Quant'].sum()
	source = source.sort_values(by=['Tiker', 'Date', 'Time'])
	
	new_summ = []
	new_summ.append(source['Quant'].values[0])
	for n, item in enumerate(source['Tiker']):
		if n > 0:
			if item == source['Tiker'].values[n - 1]:
				total = source['Quant'].values[n] + new_summ[n - 1]
				new_summ.append(total)
			elif item != source['Tiker'].values[n - 1]:
				new_summ.append(source['Quant'].values[n])
	
	# Добавление новой колонки и сохранение результата в файл
	source['Summ'] = new_summ
	# source['Money'] = source['Quant'] * source['Price']
	common_file = main_dir + new_dir + '\\' + new_name
	source.to_csv(common_file, header=True, index=False, encoding='utf-8', sep=';')
	split_to_files(common_file, tickers)
	
	
def split_to_files(file, stocks):
	""" Разбиение по тикерам и сохранение в отдельные файлы """
	source = pd.read_csv(file, sep=';')
	dir_name = os.path.dirname(file) + '\\'
	for stock in stocks:
		#df = df[np.logical_not(df['Tiker'] != item)]
		df = source.loc[source['Tiker'] == stock]
		newfile = dir_name + stock + '.csv'
		df.to_csv(newfile, header=True, index=False, encoding='utf-8', sep=';')
		print(f'Создан файл {newfile}')


if __name__ == '__main__':
	name = input("Введите имя фала без расширения:\n")
	name_file = os.path.abspath(name) + '.csv'
	load_file(name_file)
