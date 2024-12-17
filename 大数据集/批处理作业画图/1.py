import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 读取三个CSV文件
df1 = pd.read_csv('glances_m.csv')
df2 = pd.read_csv('glances_d.csv')
df3 = pd.read_csv('glances_m_d.csv')

cpu_row = ['cpu_system', 'cpu_iowait', 'cpu_user']
load_row = ['load_min1', 'load_min5']
disk_row = ['diskio_sda_read_bytes', 'diskio_sda_write_bytes']
mem_row = ['mem_percent']
# 将timestamp列转换为datetime格式
for df in [df1, df2, df3]:
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    else:
        raise ValueError("Timestamp column is missing in one of the files")

# 将每个文件的时间戳从0秒开始计时
for df in [df1, df2, df3]:
    start_time = df['timestamp'].min()
    df['relative_time'] = (df['timestamp'] - start_time).dt.total_seconds()

    total_rows = len(df)
    df['relative_time'] = np.arange(0, total_rows * 2, 2)  # 每2秒递增

# 公共的指标列表
first_row = ['mem_percent', 'cpu_system', 'diskio_sda_read_bytes', 
             'diskio_sda_write_bytes', 'load_min1', 'load_min5', 
             'cpu_iowait', 'cpu_user']

# 计算时间轴最大长度，确保三个文件的时间戳对齐
max_time = min(df1['relative_time'].max(), df2['relative_time'].max(), df3['relative_time'].max())
print(df1['relative_time'].max(),df2['relative_time'].max(),df3['relative_time'].max())

# df1 = df1[df1['relative_time'] <= max_time]
# df2 = df2[df2['relative_time'] <= max_time]
# df3 = df3[df3['relative_time'] <= max_time]

for i in range(len(mem_row)):
    tmp = mem_row[i]
    if tmp in df1.columns and tmp in df2.columns and tmp in df3.columns:
    # 绘制CPU使用率随时间的变化
        plt.figure(figsize=(12, 6))
        
        plt.plot(df1['relative_time'], df1[tmp], label='MEMORY_ONLY')
        plt.plot(df2['relative_time'], df2[tmp], label='DISK_ONLY')
        plt.plot(df3['relative_time'], df3[tmp], label='MEMORY_AND_DISK')
            
        plt.title(f'{tmp} Comparison')
        plt.xlabel('Time (s)')
        plt.ylabel(f"{tmp} %")
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        filename = f"{tmp}_comparison.png"
        plt.savefig(filename)
        plt.show()
        print(f"Saved figure: {filename}")
        plt.close()

for i in range(len(cpu_row)):
    tmp = cpu_row[i]
    if tmp in df1.columns and tmp in df2.columns and tmp in df3.columns:
    # 绘制CPU使用率随时间的变化
        plt.figure(figsize=(12, 6))
        
        plt.plot(df1['relative_time'], df1[tmp], label='MEMORY_ONLY')
        plt.plot(df2['relative_time'], df2[tmp], label='DISK_ONLY')
        plt.plot(df3['relative_time'], df3[tmp], label='MEMORY_AND_DISK')
            
        plt.title(f'{tmp} Comparison')
        plt.xlabel('Time (s)')
        plt.ylabel(f'{tmp} %')
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        filename = f"{tmp}_comparison.png"
        plt.savefig(filename)
        plt.show()
        print(f"Saved figure: {filename}")
        plt.close()

for i in range(len(load_row)):
    tmp = load_row[i]
    if tmp in df1.columns and tmp in df2.columns and tmp in df3.columns:
        plt.figure(figsize=(12, 6))
        plt.plot(df1['relative_time'], df1[tmp], label='MEMORY_ONLY')
        plt.plot(df2['relative_time'], df2[tmp], label='DISK_ONLY')
        plt.plot(df3['relative_time'], df3[tmp], label='MEMORY_AND_DISK')
        plt.xlabel('Time (s)')
        plt.ylabel(f'{tmp}')
        plt.title('System Load Over Time')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid()
        plt.tight_layout()
        filename = f"{tmp}_comparison.png"
        plt.savefig(filename)
        plt.show()
        print(f"Saved figure: {filename}")
        plt.close()

for i in range(len(disk_row)):
    tmp = disk_row[i]
    if tmp in df1.columns and tmp in df2.columns and tmp in df3.columns:
        plt.figure(figsize=(12, 6))
        plt.plot(df1['relative_time'], df1[tmp], label='MEMORY_ONLY')
        plt.plot(df2['relative_time'], df2[tmp], label='DISK_ONLY')
        plt.plot(df3['relative_time'], df3[tmp], label='MEMORY_AND_DISK')
        plt.xlabel('Time (s)')
        plt.ylabel(f'{tmp}')
        plt.title('Disk I/O Over Time')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid()
        plt.tight_layout()
        filename = f"{tmp}_comparison.png"
        plt.savefig(filename)
        plt.show()
        print(f"Saved figure: {filename}")
        plt.close()
