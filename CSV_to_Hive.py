import pandas as pd
from pyspark.sql import SparkSession
import getpass


def main():
    courses = {'course_id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
               'course_name': ['Mastering SQL', 'Streaming Pipelines - Python', 'Head First Python',
                               'Designing Data-Intensive Applications', 'Distributed Systems', 'Database Internals',
                               'Art of Immutable Architecture', 'Graph Databases', 'Building MicroServices',
                               'Kubernetes Patterns'],
               'course_author': ['Mike Jack', 'Bob Davis', 'Elvis Presley', 'Martin Kleppmann', 'Sukumar Ghosh',
                                 'Alex Petrov',
                                 'Michael L. Perry', 'Ian Robinson', 'Sam Newman', 'Rolan Hub'],
               'course_status': ['   published   ', '   inactive   ', '  draft   ', 'published  ', '   draft',
                                 '   inactive',
                                 'published   ', '   draft', '  inactive ', 'published   '],
               'course_published_dt': ['2020-07-08', '2020-03-10', '\\N', '2021-02-27', '\\N', '2021-05-14',
                                       '2021-04-18', '\\N',
                                       '2020-12-15', '2021-07-11']}

    df = pd.DataFrame(courses, columns=['course_id', 'course_name', 'course_author', 'course_status',
                                        'course_published_dt'])  # create DataFrame

    df.to_csv('/home/itv000558/courses/Courses.csv', sep='\u0001', index=False, header=True)

    username = getpass.getuser()

    spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        appName(f'{username} | Python - Data Processing - Overview'). \
        master('yarn'). \
        getOrCreate()

    course_df = spark.read. \
        csv("/user/itv000558/courses/Courses.csv",
            sep='\u0001',
            header=True
            )
    spark_df = spark.read.table("itv000558_courses.courses")

    dtypes_casted = list(map(lambda dtype: f' trim(cast({dtype[0]} as {dtype[1]})) as {dtype[0]} ', spark_df.dtypes))

    course_df = course_df.selectExpr(*dtypes_casted)

    course_df.write. \
        format("hive"). \
        saveAsTable("itv000558_courses.courses", mode="append")

    spark.sql("select * from itv000558_courses.courses").show(truncate=False)


if __name__ == "__main__":
    main()
