from pyspark import SparkConf, SparkContext, StorageLevel
import sys
import time
import os
import shutil

output_path = "/home/dase/pd/res"
storage_level_param = "MEMORY_AND_DISK"
num_copies = 2

def delete_folder(folder_path):
    # 检查文件夹是否存在
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        try:
            # 递归删除文件夹
            shutil.rmtree(folder_path)
            print(f"文件夹 {folder_path} 已被删除。")
        except Exception as e:
            print(f"删除文件夹时出错：{e}")
    else:
        print(f"文件夹 {folder_path} 不存在。")

def word_count_with_persistence(output_path, storage_level_param, num_copies):
    # 配置 Spark
    conf = SparkConf().setAppName("WordCountPySpark").setMaster("spark://hadoop1:7077")
    # conf = SparkConf().setAppName("WordCountPySpark").setMaster("local")
    sc = SparkContext(conf=conf)
    input_path= "hdfs://hadoop1:9820/output.test"
    # 定义存储级别映射
    storage_levels = {
        "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
        "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
        # "MEMORY_ONLY_SER": StorageLevel.MEMORY_ONLY_SER,
        # "MEMORY_AND_DISK_SER": StorageLevel.MEMORY_AND_DISK_SER,
        "DISK_ONLY": StorageLevel.DISK_ONLY,
        "MEMORY_ONLY_2": StorageLevel.MEMORY_ONLY_2,
        "MEMORY_AND_DISK_2": StorageLevel.MEMORY_AND_DISK_2
    }

    # 解析存储级别
    if storage_level_param not in storage_levels:
        print("Invalid storage level. Use one of: MEMORY_ONLY, MEMORY_AND_DISK, MEMORY_ONLY_SER, "
              "MEMORY_AND_DISK_SER, DISK_ONLY, MEMORY_ONLY_2, MEMORY_AND_DISK_2")
        sys.exit(1)

    storage_level = storage_levels[storage_level_param]

    # 读取数据集
    lines = sc.textFile(input_path)

    # 记录起始时间

    # 记录起始时间
    # start_time = time.time()

    # 复制 lines 并合并
    copied_lines = lines
    for _ in range(num_copies - 1):  # 复制 num_copies - 1 次
        copied_lines = copied_lines.union(lines)

    # 将 RDD 持久化到指定存储级别
    copied_lines.persist(storage_level)

    # 处理文本数据并计算词频（执行多个阶段的计算）
    for _ in range(3):  # 多次执行词频统计，模拟多次计算
        start_time = time.time()
        word_counts = copied_lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False)
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # 转换为毫秒
        print(f"Execution time with {storage_level_param}: {execution_time:.2f} ms")

        # 如果是第一次计算，保存到输出路径
        # if _ == 0:
            # word_counts.saveAsTextFile(output_path)

    # 记录结束时间并计算执行时间
    # end_time = time.time()
    # execution_time = (end_time - start_time) * 1000  # 转换为毫秒
    # print(f"Execution time with {storage_level_param}: {execution_time:.2f} ms")

    # 停止 SparkContext
    copied_lines.unpersist()


    sc.stop()


delete_folder(output_path)
word_count_with_persistence(output_path, storage_level_param, num_copies)

# if __name__ == "__main__":
#     if len(sys.argv) != 4:
#         print("Usage: python word_count_with_persistence.py <input-path> <output-path> <storage-level>")
#         sys.exit(1)

#     input_path = sys.argv[1]
#     output_path = sys.argv[2]
#     storage_level_param = sys.argv[3].upper()

#     word_count_with_persistence(input_path, output_path, storage_level_param)
