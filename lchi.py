import pandas as pd
import numpy as np
import os


def load_file(file_name):
    directory = os.path.dirname(file_name) + '\\'
    source = pd.read_csv(file_name, sep=" |;")
    source.columns = ['Date', 'Time', 'Tiker', 'Quant', 'Price']
    tickers = sorted(source['Tiker'].unique().tolist())
    source = source.groupby(['Date', 'Time', 'Tiker', 'Price'], as_index=False)['Quant'].sum()
    source = source.sort_values(by=['Tiker', 'Date', 'Time'])
    
    summ = []
    summ.append(source['Quant'].values[0])
    for n, item in enumerate(source['Tiker']):
        if n > 0:
            if item == source['Tiker'].values[n-1]:
                total = source['Quant'].values[n] + summ[n - 1]
                summ.append(total)
            elif item != source['Tiker'].values[n-1]:
                summ.append(source['Quant'].values[n])
        

    # Добавление новой колонки и сохранение результата в файл
    source['Summ'] = summ
    common_file = directory + file_name.split('\\')[-1][2:]
    source.to_csv(common_file, header=True, index=False, encoding='utf-8', sep=';')

    # Разбиение по тикерам и сохранение в отдельный файл
    for item in tickers:
        df = source[np.logical_not(source['Tiker'] != item)]
        newfile = directory + item + '.csv'
        df.to_csv(newfile, header=True, index=False, encoding='utf-8', sep=';')
        print(df.tail(2))
        

if __name__ == '__main__':
    name = input("Введите имя фала без расширения:\n")
    # name = '2_312371'
    name_file = os.path.abspath(name) + '.csv'
    load_file(name_file)
