from pyspark import SparkConf, SparkContext, StorageLevel
import sys
import time


def word_count_with_persistence(input_path, output_path, storage_level_param):
    # 配置 Spark
    conf = SparkConf().setAppName("WordCountPySpark").setMaster("local")
    sc = SparkContext(conf=conf)

    # 定义存储级别映射
    storage_levels = {
        "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
        "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
        "MEMORY_ONLY_SER": StorageLevel.MEMORY_ONLY_SER,
        "MEMORY_AND_DISK_SER": StorageLevel.MEMORY_AND_DISK_SER,
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
    start_time = time.time()

    # 将 RDD 持久化到指定存储级别
    lines.persist(storage_level)

    # 处理文本数据
    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))
    word_counts = pairs.reduceByKey(lambda a, b: a + b)

    # 保存结果
    word_counts.saveAsTextFile(output_path)

    # 记录结束时间并计算执行时间
    end_time = time.time()
    execution_time = (end_time - start_time) * 1000  # 转换为毫秒
    print(f"Execution time with {storage_level_param}: {execution_time:.2f} ms")

    # 停止 SparkContext
    sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python word_count_with_persistence.py <input-path> <output-path> <storage-level>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    storage_level_param = sys.argv[3].upper()

    word_count_with_persistence(input_path, output_path, storage_level_param)
