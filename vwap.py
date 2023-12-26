import pandas as pd
import numpy as np
import itertools
import os


def check_sigh(first, second) -> bool:
	"""Проверка двух чисел на одинаковость знака"""
	if (first > 0 and second > 0) or (first < 0 and second < 0):
		return True
	else:
		return False


def find_begin(df) -> list:
	"""Поиск индексов начала одной сделки"""
	list_begin = []
	len_df = df.shape[0]
	for item in df.index:
		if item == 0:
			res_qnt = check_sigh(df.Quant[item], df.Quant[item + 1])
			if df['Date'][item] == df['Date'][item + 1]:
				if df['Time'][item] == df['Time'][item + 1] and res_qnt:
					list_begin.append(item)
		elif item < len_df - 1:
			res_qnt = check_sigh(df.Quant[item], df.Quant[item + 1])
			if df['Date'][item] == df['Date'][item + 1]:
				if df['Time'][item] == df['Time'][item + 1]:
					if res_qnt:
						# if df['Summ'][item - 1] == 0:
						list_begin.append(item)
	return list_begin


def find_end(df) -> list:
	"""Поиск индексов конца одной сделки"""
	list_end = []
	for item in df.index.values:
		if item > 0 and item < df.index.values.max():
			result_quant = check_sigh(df.Quant[item], df.Quant[item - 1])
			if df['Date'][item] == df['Date'][item - 1]:
				if df['Time'][item] == df['Time'][item - 1]:
					if result_quant or df['Summ'][item - 1] == 0:
						list_end.append(item)
		elif item == df.index.values.max():
			result = check_sigh(df.Quant[item], df.Quant[item - 1])
			if df['Date'][item] == df['Date'][item - 1]:
				if df['Time'][item] == df['Time'][item - 1] and not result:
					if df['Quant'][item] + df['Quant'][item - 1] != 0:
						list_end.append(item)
	return list_end


def check_lists(_list1, _list2) -> bool:
	merged_list = list(zip(_list1, _list2))
	for i, item in enumerate(merged_list):
		if i > 0:
			last = item[1]
			if item[1] < item[0] and item[0] <= merged_list[i - 1][1]:
				return False
	return True


def load_file(file_name):
	"""Главная функция"""
	# SettingWithCopyWarning in Pandas
	pd.options.mode.chained_assignment = None
	source = pd.read_csv(file_name, sep=";")
	source['Type'] = ''
	source['New_Price'] = source['Price']
	source['New_Quant'] = source['Quant']
	source['VW'] = source['Price'] * source['Quant']
	source['VWAP'] = source['VW']
	source.insert(2, 'Type', source.pop('Type'))
	
	for item in range(source.index.size):
		if source['Quant'][item] > 0:
			source['Type'][item] = 'Buy'
		else:
			source['Type'][item] = 'Sell'
	
	source_new = source.duplicated(subset=['Date', 'Time', 'Type'])
	begin_list = []
	end_list = []
	for row, number in enumerate(source_new):
		if number == True and source_new[row - 1] == False:
			begin_list.append(row - 1)
	
	for row, number in enumerate(source_new):
		if number == True and source_new[row + 1] == False:
			end_list.append(row)
	
	if len(begin_list) == len(end_list) and len(begin_list) > 0:
		merged_list = list(zip(begin_list, end_list))
	else:
		exit("Списки не совпадают! Пишите программисту для исправления кода!")
	
	for item in merged_list:
		for num in source.index:
			if num >= item[0] and num <= item[1]:
				_end = item[1] + 1
				new_quant = source['Quant'][item[0]:_end].groupby(level=0).sum()
				new_summ = source['VW'][item[0]:_end].groupby(level=0).sum()
				if new_quant.sum() != 0:
					new_price = new_summ.sum() / new_quant.sum()
				else:
					new_price = new_summ.sum()
				
				source.at[num, 'New_Quant'] = int(new_quant.sum())
				source.at[num, 'VWAP'] = float("{:.2f}".format(new_price))
				source.at[num, 'New_Price'] = source.at[num, 'VWAP']
			
	new_list_del = make_new_list(merged_list)
	new_source = source.drop(index=new_list_del)
	result = new_source[['Date', 'Time', 'Tiker', 'Type', 'New_Price', 'New_Quant']]
	result = result.rename(columns={'New_Price': 'Price', 'New_Quant': 'Quant'})
	
	new_summ = []
	new_summ.append(result['Quant'].values[0])
	for n, item in enumerate(result['Tiker']):
		if n > 0:
			if item == result['Tiker'].values[n - 1]:
				total = result['Quant'].values[n] + new_summ[n - 1]
				new_summ.append(total)
			elif item != result['Tiker'].values[n - 1]:
				new_summ.append(source['Quant'].values[n])
	
	result["Summ"] = new_summ
	result.to_csv(file_name[:-4] + '_agr.csv', sep=':', index=False, header=True)
	print(result.head(4))


def make_new_list(merged_list) -> list:
	"""Создание списка индексов для удаления строк"""
	new_list_del = []
	for item in merged_list:
		if item[1] - item[0] == 1:
			list_del = item[0]
			new_list_del.append([list_del])
		else:
			list_del = [i for i in range(item[0], item[1])]
			new_list_del.append(list_del)
	merged = list(itertools.chain.from_iterable(new_list_del))
	return merged


def make_list_files(names):
	list_file = []
	path_name = os.getcwd() + '\\' + names + '\\'
	for item in os.listdir(path=names):
		if item[-3:] == 'csv' and len(item) < 10:
			list_file.append(path_name + item)
	return list_file


if __name__ == '__main__':
	name = input("Введите имя папки или имя файла:\n")
	if name[-4:-3] == '.':
		if name[-3:] == 'csv':
			load_file(os.getcwd() + '\\' + name)
		else:
			print(f"Такого файла {os.path.abspath(name)} не существует")
	else:
		if os.path.exists(name):
			list_files = make_list_files(name)
			for file in list_files:
				load_file(file)
		else:
			print(f"Такой папки {os.path.abspath(name)} не существует")
