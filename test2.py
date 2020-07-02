import pandas as pd
from tqdm import tqdm                  # 显示进度条
from multiprocessing import Pool
import time
import os
#数据处理函数
def data_process(data):
    for index,row in tqdm(data.iterrows()):
        if row['ONTIME']!=row['WORKTIME']+row['STOPTIME']:
            data.drop(index=index,axis=0,inplace=True)
    return data

if __name__=='__main__':

    # 导入数据
    file_path = 'C:/Users/xingyahui/Desktop/mass/2019-2020第二学期/大数据分析/天正设备运行数据/MONTH_04/检测线数据/MACHINE_ID_12.csv'
    data = pd.DataFrame(pd.read_csv(file_path))

    # 数据预处理
    data.UPDATE_DATE = pd.to_datetime(data.UPDATE_DATE)
    data.set_index(["UPDATE_DATE"], inplace=True)
    data.sort_index()
    t0 = time.time()
    #data_process(data)
    #print("单行处理花费时间{t}s".format(t =time.time() - t0))

    # 四核，将所有数据集分成四个子数据集
    Len_data = len(data.index)
    subset1 = data.iloc[:(Len_data // 4)]
    subset2 = data.iloc[(Len_data // 4): (Len_data // 2)]
    subset3 = data.iloc[(Len_data // 2): ((Len_data * 3) // 4)]
    subset4 = data.iloc[((Len_data * 3) // 4):]
    subsets = [subset1, subset2, subset3, subset4]

    p = Pool()
    t1 = time.time()
    data_num = []
    for i in range(4):
        res = p.apply_async(data_process, args=(subsets[i],))
        data_num.append(res)
    p.close()
    p.join()

    df = []
    k = [data_num[0].get(),data_num[1].get(),data_num[2].get(),data_num[3].get()]
    df = pd.concat(k, axis=0)
    df.to_csv('C:/Users/xingyahui/Desktop/mass/2019-2020第二学期/大数据分析/天正设备运行数据/MONTH_04_CHECKED/检测线数据/MACHINE_ID_12.csv', index=True, header=True)
    print(df)
    print("并行处理花费时间{t}s".format(t = time.time() - t1))


