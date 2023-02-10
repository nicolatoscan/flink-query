# %% imports
import matplotlib.pyplot as plt
# import seaborn as sns
import pandas as pd
import os

# %% read result file
# headers = ["Workername","cnt","id","ts","txts","rxts","latency","bytes","latencyBreakdown","processorId"]
headers = ["MsgId","txt","ts","cnt","meter","latency_max","latency_mean","latency_min","latency_std","latency_comp","isEnd"]

sink_metric_path = "./metrics_logs/"
sink_metric_files = os.listdir(sink_metric_path)
i = 0
data_pd = pd.DataFrame(columns=['processorId', 'file', 'msgId', 'txt', 'ts', 'cnt', 'meter', 'latency_max', 'latency_mean', 'latency_min', 'latency_std', 'latency_comp', 'isEnd'])

for fs in sink_metric_files:
    if fs.startswith("metrics_"):
        i += 1
        with open(sink_metric_path + fs, 'r') as f:
            for j, line in enumerate(f):
                [MsgId, txt, ts, cnt, meter, latency_max, latency_mean, latency_min, latency_std, latency_comp, isEnd] = line.split(',')

                tmp_pd = pd.DataFrame([[i, fs, int(MsgId), txt, int(ts), int(cnt), float(meter), float(latency_max), float(latency_mean), float(latency_min), float(latency_std), float(latency_comp), isEnd]], columns=['processorId', 'file', 'msgId', 'txt', 'ts', 'cnt', 'meter', 'latency_max', 'latency_mean', 'latency_min', 'latency_std', 'latency_comp', 'isEnd'])
                
                 
                data_pd = pd.concat([data_pd, tmp_pd])
                #  data_pd = pd.concat([data_pd, ], )
                # break

# print(tmp_pd.head())
# store first
print(data_pd.head())
data_pd.to_csv("./results/test.csv")
data_pd[['ts']].plot.line()


# data = []
# with open('sink.csv', 'r') as f:
#     for line in f:
#         [ _, workername, cnt, id, ts, txts, rxts, latency, bytes, latencyBreakdown, processorId, shish ] = line.split(',')
#         data.append({
#             "Workername": workername,
#             "cnt": int(cnt),
#             "id": int(id),
#             "ts": int(ts),
#             "txts": int(txts),
#             "rxts": int(rxts),
#             "latency": int(latency),
#             "bytes": int(bytes),
#             "latencyBreakdown": [ int(x) for x in latencyBreakdown.split(';') ],
#             "processorId": int(processorId),
#         })

# pIds = set( [ d['processorId'] for d in data ] )
# data[0]['diff'] = 0
# for i in range(1, len(data)):
#     data[i]['diff'] = data[i]['rxts'] - data[i-1]['rxts']

# %% plot fun
def plot(attrib: str):
    plt.plot( [ d[attrib] for d in data ] )
    # plt.show()
    # print([ d[attrib] for d in data ])
    for i, pId in enumerate(pIds):
        plt.plot([ d[attrib] if d['processorId'] == pId else None for d in data ], label=f'Worker {i+1} - Id:{pId}')
    # add labels
    plt.xlabel('tuple')
    plt.ylabel('latency [ms]')
    plt.legend()
    plt.show()

# %% plot diff
# plot('latency_max')

# %%