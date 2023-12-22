import pandas as pd
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
				if df['Time'][item] != df['Time'][item - 1]:
					if df['Time'][item] == df['Time'][item + 1]:
						if res_qnt or df['Summ'][item - 1] == 0:
							if df['Quant'][item] + df['Quant'][item + 1] != 0:
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
					if df['Time'][item] != df['Time'][item + 1] or df['Summ'][item - 1] == 0:
						if result_quant:
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
	source = pd.read_csv(file_name, sep=";")
	source['New_Price'] = source['Price']
	source['New_Quant'] = source['Quant']
	source['VW'] = source['Price'] * source['Quant']
	source['VWAP'] = source['VW']
	
	# df = pd.DataFrame()
	# df['Date'] = source['Date']
	# df['Time'] = source['Time']
	
	list_begin = find_begin(source)
	list_end = find_end(source)
	if check_lists(list_begin, list_end):
		if len(list_begin) == len(list_end):
			merged_list = list(zip(list_begin, list_end))
		else:
			len_min = min(len(list_begin), len(list_end))
			merged_list = list(zip(list_begin[:len_min], list_end[:len_min]))
	else:
		exit("Невозможно разделить сделки! Измените код!")
	
	print(merged_list)
	
	for item in merged_list:
		for num in source.index:
			if num >= item[0] and num <= item[1]:
				one_deal = source.at[item[0], 'Quant'] + source.at[item[1], 'Quant']
				if item[1] - item[0] == 1 and one_deal == 0:
					source.at[num, 'New_Quant'] = source.at[num, 'Quant']
					source.at[num, 'VWAP'] = source.at[num, 'VW']
					source.at[num, 'New_Price'] = source.at[num, 'Price']
				else:
					summ = source.loc[source.index[item[0]:item[1] + 1]].VW
					quant = source.loc[source.index[item[0]:item[1] + 1]].Quant
					if quant.sum() != 0:
						vwap = summ.sum() / quant.sum()
					else:
						vwap = summ.sum()
					source.at[num, 'New_Quant'] = int(quant.sum())
					source.at[num, 'VWAP'] = float("{:.2f}".format(vwap))
					source.at[num, 'New_Price'] = source.at[num, 'VWAP']
	
	new_list_del = make_new_list(merged_list)
	new_source = source.drop(index=new_list_del)
	result = new_source[['Date', 'Time', 'Tiker', 'New_Price', 'New_Quant']]
	save_file(file_name.split('\\')[-1][:-4], result)
	print(result)


def save_file(_file, df):
	name_file = os.getcwd() + '\\' + _file + '_lchi.csv'
	df.to_csv(name_file, sep=';', index=False, header=True)


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


if __name__ == '__main__':
	# name = input("Введите имя фала без расширения:\n")
	name = 'ALZ2'
	name_file = os.path.abspath(name) + '.csv'
	load_file(name_file)
