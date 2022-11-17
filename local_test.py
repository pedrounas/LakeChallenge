from pathlib import Path

from pyspark.context import SparkContext
from pyspark.sql import SparkSession


def parse_file(file, output):
    df = spark \
        .read \
        .option('delimeter', ',') \
        .option('header', True) \
        .option('encoding', 'UTF-8') \
        .csv(str(file))

    df = df.withColumnRenamed('Origin Movement ID', 'origin_id'). \
        withColumnRenamed('Origin Display Name', 'origin_name'). \
        withColumnRenamed('Origin Geometry', 'origin_geometry'). \
        withColumnRenamed('Destination Movement ID', 'destination_id'). \
        withColumnRenamed('Destination Display Name', 'destination_name'). \
        withColumnRenamed('Destination Geometry', 'destination_geometry'). \
        withColumnRenamed('Date Range', 'date_range'). \
        withColumnRenamed('Mean Travel Time (Seconds)', 'mean_travel_time'). \
        withColumnRenamed('Range - Lower Bound Travel Time (Seconds)', 'lower_travel_time'). \
        withColumnRenamed('Range - Upper Bound Travel Time (Seconds)', 'upper_travel_time')

    df = df.withColumn('origin_id', df.origin_id.cast('int')). \
        withColumn('destination_id', df.destination_id.cast('int')). \
        withColumn('mean_travel_time', df.mean_travel_time.cast('int')). \
        withColumn('lower_travel_time', df.lower_travel_time.cast('int')). \
        withColumn('upper_travel_time', df.upper_travel_time.cast('int'))

    tmp_path = output.joinpath('city=' + f.stem.split(' - ')[-1])
    tmp_path.mkdir(parents=True, exist_ok=True)
    df.write.mode('overwrite').parquet(str(tmp_path.joinpath('travel_times.parquet')))


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    path = Path('./data/')
    file_list = path.glob('*.csv')
    output_path = Path('./output/')
    for f in file_list:
        parse_file(f, output_path)
