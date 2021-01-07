# -*- coding: utf-8 -*- 
# @Time : 2020/12/2 10:12 
# @Author : Li Xiaopeng
# @File : constants.py
# @Software: PyCharm


import platform

from pyspark.sql import SparkSession


def sparkSession(name=None, local_core_num=3, local_memory_gb=4):
    sparkBuilder = SparkSession.builder.appName(name)
    if "win" in platform.system().lower():
        sparkBuilder.master("local[%s]" % local_core_num) \
            .config("spark.driver.memory", "%sg" % local_memory_gb)
    else:
        pass
    return sparkBuilder.getOrCreate()


if __name__ == "__main__":
    print(platform.system().lower())
    spark = sparkSession()
    df = spark.sql("select 'test sql in spark' as id")
    df.show()

