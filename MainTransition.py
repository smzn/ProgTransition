import pandas as pd
import datetime as dt
import time
from mpi4py import MPI
import sys
import utils

class MainTransition:

    def __init__(self, ap_file, data_file, calmonth, rank, size, path):
        self.df_ap = utils.getCSV(ap_file)
        #print(self.df_ap.head())
        self.df_data = utils.getCSV(data_file)
        #print(self.df_data.head())
        self.calmonth = calmonth
        self.rank = rank
        self.size = size
        self.path = path

        self.df_unique = utils.getDuplicate(self.df_data, 'client')
        if self.rank == 0:
            print('ユニーク利用者数 : ' +str(len(self.df_unique)))

        self.transition_from = []
        self.transition_to = []
        self.duration = []
        self.client = []
        self.transition = [[0 for i in range(len(self.df_ap)+1)] for i in range(len(self.df_ap)+1)]
        

    def getTransition(self):
        #3.
        l_div = [(len(self.df_unique['client'])+i) // self.size for i in range(self.size)]
        #print(l_div)
        start = 0
        end = 0
        for i in range(self.rank):
            start += l_div[i]
        for i in range(self.rank + 1):
            end += l_div[i]
        div_unique = self.df_unique[start:end]
        print('rank = {0}, size = {1}, start = {2}, end = {3}'.format(self.rank, self.size, start, end))

        #for i, val in enumerate(self.df_unique['client']):
        for i, val in enumerate(div_unique['client']):
            #print(str(i) + '/' + str(len(self.df_unique)))
            #print(str(i) + '/' + str(len(div_unique)))
            from_ap = len(self.df_ap)
            to_ap = -1
            for j in self.df_data.itertuples():
                if(j.client == val):
                    if(to_ap == -1):
                        to_ap = j.AP
                        from_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S')
                        to_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S')
                        self.transition_from.append(from_ap)
                        self.transition_to.append(to_ap)
                        self.duration.append(-1)
                        self.client.append(val)
                    elif(to_ap >= 0):
                        if(to_ap == j.AP):
                            to_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S')
                        else:
                            self.transition_from.append(to_ap)
                            self.transition_to.append(j.AP)
                            to_time = dt.datetime.strptime(j.timestamp, '%Y-%m-%d %H:%M:%S')
                            delta = to_time - from_time
                            self.duration.append(delta.total_seconds())
                            self.client.append(val)

                            from_ap = to_ap
                            to_ap = j.AP
                            from_time = to_time

            self.transition_from.append(to_ap)
            self.transition_to.append(len(self.df_ap))
            delta = to_time - from_time
            self.duration.append(delta.total_seconds())
            self.client.append(val)

#            if(i > 30):
#                break
        #4. 
        df = pd.DataFrame({'client': self.client, 'from': self.transition_from, 'to': self.transition_to, 'duration': self.duration})
        utils.saveCSV(df, self.path +'/main/'+str(self.calmonth)+'/duration_'+str(self.calmonth)+'_'+str(self.rank)+'.csv')

        #5.
        for i, j in zip(self.transition_from, self.transition_to):
            self.transition[i][j] += 1
        utils.saveCSVi(self.transition, self.path +'/main/'+str(self.calmonth)+'/transition_'+str(self.calmonth)+'_'+str(self.rank)+'.csv')

        
if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    path = '.'
    calmonth = sys.argv[1]
    maintransition = MainTransition(path +'/traceset/APlocations.csv', path +'/pre/'+calmonth+'.csv', calmonth, rank, size, path)
    start = time.time()
    maintransition.getTransition()
    elapsed_time = time.time() - start
    print ("Rank : {0}, Transition_Compute_time:{1}".format(rank, elapsed_time) + "[sec]")
