import pandas as pd
import datetime as dt
import time

import sys
sys.path.append('/content/drive/MyDrive/研究/WiFiLog/')
import utils

class PreTransition:
    def __init__(self, ap_file, data_file, calmonth, path):
        self.df_ap = utils.getCSV(ap_file)
        self.df_data = utils.getCSV(data_file)
        self.calmonth = calmonth
        self.path = path
        
        
    def getPreTransition(self):
        #2
        print(self.df_data.isnull().sum())
        self.df_data = self.df_data[self.df_data['AP'].isnull() == False]
        print(self.df_data.isnull().sum())

        #3
        grouped = self.df_data.groupby('client').size()
        print(grouped)
        l_columns = list(grouped[grouped > 1].index)
        self.df_data_over2 = self.df_data[self.df_data['client'].isin(l_columns)].copy()
        grouped2 = self.df_data_over2.groupby('client').size().sort_values(ascending=True)
        print(grouped2)

        #4
        df_ap_index = pd.DataFrame({'AP': self.df_ap.AP, 'AP_index': range(len(self.df_ap.AP))})
        self.df_data_over2 = self.df_data_over2.merge(df_ap_index, on = 'AP', how = 'left')
        self.df_data_over2.drop(columns='AP', inplace=True)
        self.df_data_over2.rename(columns={'AP_index': 'AP'}, inplace=True)
        print(self.df_data_over2)

        #5
        utils.saveCSV(self.df_data_over2, path +'/pre/'+str(self.calmonth)+'.csv')

    def getClientsize(self):
        grouped = self.df_data.groupby('client').size().sort_values(ascending=False)
        utils.saveCSVi(grouped, path +'/pre/clientsize_'+str(self.calmonth)+'.csv')


if __name__ == '__main__':
    path = '/content/drive/MyDrive/研究/WiFiLog/wifidata'
    file = ['2014_01', '2014_02', '2014_03', '2014_04', '2014_05', '2014_06', '2014_07', '2014_08', '2014_09', '2014_10', '2014_11', '2014_12', '2015_01', '2015_02', '2015_03', '2015_04']
    month = ['201401','201402','201403','201404','201405','201406','201407','201408','201409','201410','201411','201412','201501','201502','201503','201504']
    for i, j in zip(file, month): 
        print(i)
        pretransition = PreTransition(path + '/traceset/APlocations.csv', path + '/traceset/'+i+'.csv', j, path)
        start = time.time()
        pretransition.getPreTransition()
        elapsed_time = time.time() - start
        print("calclation_time:{0}".format(elapsed_time)+"[sec]")
        pretransition.getClientsize()

 