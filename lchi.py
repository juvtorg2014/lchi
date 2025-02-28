import pandas as pd
import numpy as np
import os


def save_file(source, main, new, old_name):
	common_file = main + new + '\\' + old_name + '.csv'
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
	source['Deal'] = np.where(source['Quant'] > 0, 'Long', 'Short')
	# save_file(source, main_dir, new_dir, name)
	
	tickers = sorted(source['Tiker'].unique().tolist())
	source = source.groupby(['Date', 'Time', 'Tiker', 'Price', 'Deal'], as_index=False)['Quant'].sum()
	df = source.sort_values(by=['Tiker', 'Date', 'Time'])

	# Группировка сделок по времени и направлению
	new_df = df.groupby(['Date', 'Time', 'Tiker', 'Deal'], as_index=False).agg({'Price':'mean', 'Quant':'sum'})
	new_df = new_df.sort_values(by=['Tiker', 'Date', 'Time'])
	
	new_summ = count_summ(new_df)

	# Добавление новой колонки и сохранение результата в файл
	new_df['Summ'] = new_summ
	# source['Money'] = source['Quant'] * source['Price']
	common_file = main_dir + new_dir + '\\' + new_name
	new_df.to_csv(common_file, header=True, index=False, encoding='utf-8', sep=';')
	split_to_files(common_file, tickers)
	
	
def count_summ(df) -> list:
	"""Подсчет сумм сделок для новой колонки"""
	_summ = []
	_summ.append(df['Quant'].values[0])
	for num, item in enumerate(df['Tiker']):
		if num > 0:
			if item == df['Tiker'].values[num - 1]:
				total = df['Quant'].values[num] + _summ[num - 1]
				_summ.append(total)
			elif item != df['Tiker'].values[num - 1]:
				_summ.append(df['Quant'].values[num])
	return _summ
	
	
def split_to_files(file, stocks):
	""" Разбиение по тикерам и сохранение в отдельные файлы """
	source = pd.read_csv(file, sep=';')
	dir_name = os.path.dirname(file) + '\\'
	for stock in stocks:
		df = source.loc[source['Tiker'] == stock]
		newfile = dir_name + stock + '.csv'
		df.to_csv(newfile, header=True, index=False, encoding='utf-8', sep=';')
		print(f'Создан файл {newfile}')


if __name__ == '__main__':
	name = input("Введите имя фала без расширения:\n")
	name_file = os.path.abspath(name) + '.csv'
	if os.path.exists(name_file):
		load_file(name_file)
		print(f"Все сделано за участника {name}")
	else:
		print("Нет такого файла! Повтор!!!")

