import pandas as pd
import numpy as np
import os


def load_file(file_name):
    main_dir = os.path.dirname(file_name) + '\\'
    new_dir = file_name.split('\\')[-1][:-4]
    new_name = file_name.split('\\')[-1][2:]
    if not os.path.exists(main_dir + new_dir):
        os.mkdir(main_dir + new_dir)
    source = pd.read_csv(file_name, sep=" |;")
    source.columns = ['Date', 'Time', 'Tiker', 'Quant', 'Price']
    tickers = sorted(source['Tiker'].unique().tolist())
    source = source.groupby(['Date', 'Time', 'Tiker', 'Price'], as_index=False)['Quant'].sum()
    source = source.sort_values(by=['Tiker', 'Date', 'Time'])
    
    new_summ = []
    new_summ.append(source['Quant'].values[0])
    for n, item in enumerate(source['Tiker']):
        if n > 0:
            if item == source['Tiker'].values[n-1]:
                total = source['Quant'].values[n] + new_summ[n - 1]
                new_summ.append(total)
            elif item != source['Tiker'].values[n-1]:
                new_summ.append(source['Quant'].values[n])
        
    # Добавление новой колонки и сохранение результата в файл
    source['Summ'] = new_summ
    common_file = main_dir + new_dir + '\\' + new_name
    source.to_csv(common_file, header=True, index=False, encoding='utf-8', sep=';')

    # Разбиение по тикерам и сохранение в отдельный файл
    for item in tickers:
        df = source[np.logical_not(source['Tiker'] != item)]
        newfile = main_dir + new_dir + '\\' + item + '.csv'
        df.to_csv(newfile, header=True, index=False, encoding='utf-8', sep=';')
        print(df.tail(2))
        

if __name__ == '__main__':
    name = input("Введите имя фала без расширения:\n")
    name_file = os.path.abspath(name) + '.csv'
    load_file(name_file)
