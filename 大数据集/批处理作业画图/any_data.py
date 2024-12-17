import pandas as pd
import matplotlib.pyplot as plt

# 读取三个CSV文件
df1 = pd.read_csv('glances_m.csv')
df2 = pd.read_csv('glances_d.csv')
df3 = pd.read_csv('glances_m_d.csv')

first_row = ['mem_percent', 'cpu_system', 'diskio_sda_read_bytes', 'diskio_sda_write_bytes', 'load_min1', 'load_min5', 'cpu_iowait', 'cpu_user']

for i in range(len(first_row)):
    tmp = first_row[i]
    if tmp in df1.columns and tmp in df2.columns and tmp in df3.columns:
        # 绘制cpu_total的折线图
        plt.figure(figsize=(10, 5))
        plt.plot(df1[tmp], label='MEMORY_ONLY')
        plt.plot(df2[tmp], label='DISK_ONLY')
        plt.plot(df3[tmp], label='MEMORY_AND_DISK')
        plt.title(tmp)
        plt.xlabel('Timestamp')
        plt.ylabel('CPU Total (%)')
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    else:
        print("Error with {}".format(tmp))

# tmp = 'diskio_sda_read_count'
#
# if tmp in df1.columns and tmp in df2.columns and tmp in df3.columns:
#     # 绘制cpu_total的折线图
#     plt.figure(figsize=(10, 5))
#     plt.plot(df1[tmp], label='MEMORY_ONLY')
#     plt.plot(df2[tmp], label='DISK_ONLY')
#     plt.plot(df3[tmp], label='MEMORY_AND_DISK')
#     plt.title('CPU Total Over Time')
#     plt.xlabel('Timestamp')
#     plt.ylabel('CPU Total (%)')
#     plt.legend()
#     plt.grid(True)
#     plt.xticks(rotation=45)
#     plt.tight_layout()
#     plt.show()
# else:
#     print("The specified column 'cpu_total' does not exist in one of the CSV files.")
