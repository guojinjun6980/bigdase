from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
import time


class SparkStorageLevelExperiment:
    def __init__(self, app_name="StorageLevelPerformanceTest"):
        """
        初始化 SparkContext
        """
        # 配置 Spark 参数
        conf = SparkConf().setAppName(app_name)

        # 创建 SparkContext
        self.sc = SparkContext(conf=conf)

    def create_dataset(self, size):
        """
        创建不同规模的数据集

        :param size: 数据集大小 ('small', 'medium', 'large')
        :return: PySpark RDD
        """
        sizes = {
            'small': 100000,
            'medium': 1000000,
            'large': 10000000
        }

        # 创建测试数据集
        data_size = sizes.get(size, 100000)
        dataset = self.sc.parallelize(range(data_size))

        return dataset

    def perform_computation(self, rdd):
        """
        执行简单计算任务（乘2操作）

        :param rdd: 输入的 RDD
        :return: 计算后的 RDD
        """
        return rdd.map(lambda x: x * 2)

    def measure_performance(self, rdd, storage_level):
        """
        测量不同存储级别下的性能

        :param rdd: 输入的 RDD
        :param storage_level: Spark 存储级别
        :return: 性能指标字典
        """
        # 持久化 RDD
        rdd.persist(storage_level)

        # 记录开始时间和系统资源
        start_time = time.time()

        # 触发计算并收集结果
        result = self.perform_computation(rdd).collect()

        # 记录结束时间和系统资源
        end_time = time.time()

        # 清理缓存
        rdd.unpersist()

        # 返回性能指标
        return {
            'storage_level': str(storage_level),
            'runtime': end_time - start_time,
            'result_size': len(result)
        }

    def run_experiment(self):
        """
        运行完整实验
        """
        # 不同数据集大小
        dataset_sizes = ['small', 'medium', 'large']

        # 不同存储级别（包含所有可用的存储级别）
        storage_levels = [
            StorageLevel.MEMORY_ONLY,  # 仅内存
            # StorageLevel.MEMORY_ONLY_SER,  # 序列化存储在内存中
            StorageLevel.MEMORY_AND_DISK,  # 内存和磁盘
            # StorageLevel.MEMORY_AND_DISK_SER,  # 序列化存储在内存和磁盘中
            StorageLevel.DISK_ONLY,  # 仅磁盘

            # 带副本的存储级别（提高容错性）
            StorageLevel.MEMORY_ONLY_2,  # 2个内存副本
            # StorageLevel.MEMORY_ONLY_SER_2,  # 2个序列化内存副本
            StorageLevel.MEMORY_AND_DISK_2,  # 2个内存和磁盘副本
            # StorageLevel.MEMORY_AND_DISK_SER_2,  # 2个序列化内存和磁盘副本
            StorageLevel.DISK_ONLY_2  # 2个磁盘副本
        ]

        # 实验结果
        experiment_results = []

        for size in dataset_sizes:
            # 创建数据集
            dataset = self.create_dataset(size)

            # 对每种存储级别进行性能测试
            for level in storage_levels:
                performance = self.measure_performance(dataset, level)
                performance['dataset_size'] = size
                experiment_results.append(performance)

        return experiment_results

    def close(self):
        """
        关闭 SparkContext
        """
        self.sc.stop()


def main():
    experiment = SparkStorageLevelExperiment()

    try:
        # 运行实验
        results = experiment.run_experiment()

        # 打印结果
        print("实验结果:")
        for result in results:
            print(f"数据集大小: {result['dataset_size']}")
            print(f"存储级别: {result['storage_level']}")
            print(f"运行时间: {result['runtime']:.4f} 秒")
            print(f"结果大小: {result['result_size']} 条")
            print("-" * 40)

    finally:
        # 确保关闭 SparkContext
        experiment.close()


if __name__ == "__main__":
    main()
