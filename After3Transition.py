import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = 'IPAexGothic'
import sys
sys.path.append('/content/drive/MyDrive/研究/WiFiLog/')
import utils

class After3Transition:
    def __init__(self, calmonth, path):
        self.calmonth = calmonth
        self.path = path
        self.group_duration = utils.getCSV(self.path+'/group_duration_'+str(self.calmonth)+'.csv')
        #self.ap_duration = utils.getCSV(self.path+'/duration_all_'+str(self.calmonth)+'.csv')
        #print(self.ap_duration)
        
    def getGroupAggregation(self):
        clients_list = []
        node_list = []
        duration_list = []
        duration_sum = 0
        for row in self.group_duration.itertuples():
          if pd.isnull(row[2]): #取り出した行がNULLの場合はontinue
            continue
          elif int(row[2]) == 49:#外部から到着
            #print('outside')
            client = row[1] #client
            node = int(row[3]) #toを入れておく
            duration_sum = 0 #0に初期化
          elif int(row[3]) == node: #同じノードへの推移
            duration_sum += float(row[4])
          elif int(row[3]) != node: #他のノードへ移動
            duration_sum += float(row[4]) #たしてからリストに入れる
            clients_list.append(row[1])#リストへ登録
            node_list.append(node)
            duration_list.append(duration_sum)
            node = int(row[3]) #ノードの更新
            duration_sum = 0
         
        self.group_duration_aggregation = pd.DataFrame({'client': clients_list, 'node': node_list, 'duration': duration_list})
        utils.saveCSV(self.group_duration_aggregation, self.path +'/group_duration_aggregation_'+str(self.calmonth)+'.csv')
        
        #集約したデータフレームから拠点毎の平均滞在時間を算出
        #groupのIDは1~48,49は外部、0は使っていない
        node_number = 50
        staytime = np.zeros(node_number)
        staycount = np.zeros(node_number)
        meanstay = np.zeros(node_number)
        for row in self.group_duration_aggregation.itertuples():
            staytime[int(row.node)] += float(row.duration)
            staycount[int(row.node)] += 1
        for i, val in enumerate(staycount):
            if val > 0:
                meanstay[i] = staytime[i] / val
        utils.saveCSVi(meanstay, self.path +'/group_mean_duration_'+str(self.calmonth)+'.csv')

    ''' 挙動がおかしいので確認必要
    def getAPAggregation(self):
        clients_list = []
        node_list = []
        duration_list = []
        duration_sum = 0
        for row in self.ap_duration.itertuples():
          if pd.isnull(row[2]): #取り出した行がNULLの場合はontinue
            continue
          elif int(row[2]) == 1123:#外部から到着
            print('outside')
            client = row[1] #client
            node = row[3] #toを入れておく
            duration_sum = 0 #0に初期化
          elif int(row[3]) == node: #同じノードへの推移
            duration_sum += float(row[4])
          elif int(row[3]) != node: #他のノードへ移動
            duration_sum += float(row[4]) #たしてからリストに入れる
            clients_list.append(row[1])#リストへ登録
            node_list.append(node)
            duration_list.append(duration_sum)
            node = int(row[3]) #ノードの更新
            duration_sum = 0
         
        self.ap_duration_aggregation = pd.DataFrame({'client': clients_list, 'node': node_list, 'duration': duration_list})
        utils.saveCSV(self.ap_duration_aggregation, self.path +'/ap_duration_aggregation_'+str(self.calmonth)+'.csv')
    '''

if __name__ == '__main__':
    calmonth = sys.argv[1]
    path = '/content/drive/MyDrive/研究/WiFiLog/wifidata/after/'+str(calmonth)
    after3 = After3Transition(calmonth, path)
    after3.getGroupAggregation()
    #after3.getAPAggregation()
