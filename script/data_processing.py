import matplotlib.pyplot as plt
# import seaborn as sns
import pandas as pd
import os
import tqdm as tqdm

# # %% read result file
# headers = ["Workername","cnt","id","ts","txts","rxts","latency","bytes","latencyBreakdown","processorId"]
# headers = ["MsgId","txt","ts","cnt","meter","latency_max","latency_mean","latency_min","latency_std","latency_comp","isEnd"]

sink_metric_path = "./metrics/"
sink_metric_files = os.listdir(sink_metric_path)

# # New_Sink10_1_NO_metrics_NR
i = 0
data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId',  'ts', 'cnt', 'meter', 'bytes'])

for fs in tqdm.tqdm(sink_metric_files):
    if fs.startswith("New_Sink10_1_NO_metrics_NR"):
        i += 1
        tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
        tmp_pd['processedId'] = [i] * len(tmp_pd)
        tmp_pd['file'] = [fs] * len(tmp_pd)
        
        data_pd = pd.concat([data_pd, tmp_pd])
        print(f"Finished {i}")

data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/New_NO_metrics_NR.csv")


# # New_Sink10_1_ALO_metrics_FT
i = 0
data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId',  'ts', 'cnt', 'meter', 'bytes'])

for fs in tqdm.tqdm(sink_metric_files):
    if fs.startswith("New_Sink10_1_ALO_metrics_FT"):
        i += 1
        tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
        tmp_pd['processedId'] = [i] * len(tmp_pd)
        tmp_pd['file'] = [fs] * len(tmp_pd)
        
        data_pd = pd.concat([data_pd, tmp_pd])
        print(f"Finished {i}")

data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/New_ALO_metrics_FT.csv")


# # New_Sink10_1_EO_metrics_FT
i = 0
data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId',  'ts', 'cnt', 'meter', 'bytes'])

for fs in tqdm.tqdm(sink_metric_files):
    if fs.startswith("New_Sink10_1_EO_metrics_FT"):
        i += 1
        tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
        tmp_pd['processedId'] = [i] * len(tmp_pd)
        tmp_pd['file'] = [fs] * len(tmp_pd)
        
        data_pd = pd.concat([data_pd, tmp_pd])
        print(f"Finished {i}")

data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/New_EO_metrics_FT.csv")










# # New_Sink10_1_EO_metrics_FT
# i = 0
# data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId',  'ts', 'cnt', 'meter', 'bytes'])

# for fs in tqdm.tqdm(sink_metric_files):
#     if fs.startswith("New_Sink10_1_EO_metrics_FT"):
#         i += 1
#         tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
#         tmp_pd['processedId'] = [i] * len(tmp_pd)
#         tmp_pd['file'] = [fs] * len(tmp_pd)
        
#         data_pd = pd.concat([data_pd, tmp_pd])
#         print(f"Finished {i}")

# data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/New_EO_metrics_FT_121.csv")

# # New_Sink10_1_ALO_metrics_NR
# i = 0
# data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId',  'ts', 'cnt', 'meter', 'bytes'])

# for fs in tqdm.tqdm(sink_metric_files):
#     if fs.startswith("New_Sink10_1_ALO_metrics_NR"):
#         i += 1
#         tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
#         tmp_pd['processedId'] = [i] * len(tmp_pd)
#         tmp_pd['file'] = [fs] * len(tmp_pd)
        
#         data_pd = pd.concat([data_pd, tmp_pd])
#         print(f"Finished {i}")

# data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/New_ALO_metrics_NFT_121.csv")





# # # Sink10_1_ALO_metrics_FT
# # i = 0
# # data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId',  'ts', 'cnt', 'meter', 'bytes'])

# # for fs in tqdm.tqdm(sink_metric_files):
# #     if fs.startswith("Sink10_1_ALO_metrics_FT"):
# #         i += 1
# #         tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
# #         tmp_pd['processedId'] = [i] * len(tmp_pd)
# #         tmp_pd['file'] = [fs] * len(tmp_pd)
        
# #         data_pd = pd.concat([data_pd, tmp_pd])
# #         print(f"Finished {i}")

# # data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/ALO_metrics_FT_121.csv")

# # # Sink10_2_ALO_metrics_FT
# # i = 0
# # data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])

# # for fs in tqdm.tqdm(sink_metric_files):
# #     if fs.startswith("Sink10_2_ALO_metrics_FT"):
# #         i += 1
# #         tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
# #         tmp_pd['processorId'] = [i] * len(tmp_pd)
# #         tmp_pd['file'] = [fs] * len(tmp_pd)
        
# #         data_pd = pd.concat([data_pd, tmp_pd])
# #         print(f"Finished {i}")

# # data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/ALO_metrics_FT_1221.csv")


# # # Sink10_1_EO_metrics_FT
# # i = 0
# # data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])

# # for fs in tqdm.tqdm(sink_metric_files):
# #     if fs.startswith("Sink10_1_EO_metrics_FT"):
# #         i += 1
# #         tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
# #         tmp_pd['processorId'] = [i] * len(tmp_pd)
# #         tmp_pd['file'] = [fs] * len(tmp_pd)
        
# #         data_pd = pd.concat([data_pd, tmp_pd])
# #         print(f"Finished {i}")

# # data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/EO_metrics_FT_121.csv")


# # # Sink10_2_EO_metrics_FT
# # i = 0
# # data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])

# # for fs in tqdm.tqdm(sink_metric_files):
# #     if fs.startswith("Sink10_2_EO_metrics_FT"):
# #         i += 1
# #         tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
# #         tmp_pd['processorId'] = [i] * len(tmp_pd)
# #         tmp_pd['file'] = [fs] * len(tmp_pd)
        
# #         data_pd = pd.concat([data_pd, tmp_pd])
# #         print(f"Finished {i}")

# # data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/EO_metrics_FT_1221.csv")


# # # Sink10_1_EO_metrics_NR
# # i = 0
# # data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])

# # for fs in tqdm.tqdm(sink_metric_files):
# #     if fs.startswith("Sink10_1_EO_metrics_NR"):
# #         i += 1
# #         tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
# #         tmp_pd['processorId'] = [i] * len(tmp_pd)
# #         tmp_pd['file'] = [fs] * len(tmp_pd)
        
# #         data_pd = pd.concat([data_pd, tmp_pd])
# #         print(f"Finished {i}")

# # data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/EO_metrics_NFT_121.csv")

# # # Sink10_2_EO_metrics_NR
# # i = 0
# # data_pd = pd.DataFrame(columns=['processorId', 'file', 'latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])

# # for fs in tqdm.tqdm(sink_metric_files):
# #     if fs.startswith("Sink10_2_EO_metrics_NR"):
# #         i += 1
# #         tmp_pd = pd.read_csv(sink_metric_path + fs, names=['latency', 'msgId', 'ts', 'cnt', 'meter', 'bytes'])
# #         tmp_pd['processorId'] = [i] * len(tmp_pd)
# #         tmp_pd['file'] = [fs] * len(tmp_pd)
        
# #         data_pd = pd.concat([data_pd, tmp_pd])
# #         print(f"Finished {i}")

# # data_pd.to_csv("/mnt/d/Knowledge_Base/flink-query/results/EO_metrics_NFT_1221.csv")
