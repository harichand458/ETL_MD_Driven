import pandas as pd
from pyspark.sql import SparkSession
import getpass
import configparser as cp
from pyspark.sql.functions import when, col

def main():
    courses = {'course_id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
               'course_name': ['Mastering SQL', 'Streaming Pipelines - Python', 'Head First Python', \
                               'Designing Data-Intensive Applications', 'Ditributed Systems', 'Database Internals', \
                               'Art of Immutable Architecture', 'Graph Databases', 'Building MicroServices',
                               'Kubernetes Patterns'],
               'course_author': ['Mike Jack', 'Bob Davis', 'Elvis Presley', 'Martin Kleppmann', 'Sukumar Ghosh',
                                 'Alex Petrov', \
                                 'Michael L. Perry', 'Ian Robinson', 'Sam Newman', 'Rolan Hub'],
               'course_status': ['   published   ', '   inactive   ', '\\N', 'published  ', '\\N', '   inactive', \
                                 'published   ', '\\N', '  inactive ', 'published   '],
               'course_published_dt': ['2020-07-08', '2020-03-10', '\\N', '2021-02-27', '\\N', '2021-05-14',
                                       '2021-04-18', '\\N', \
                                       '2020-12-15', '2021-07-11']}

    users = {'user_id': ['1001', '1002', '1003', '1004', '1005', '1006'],
             'user_name': ['BenJohnson   ', '  Halley Battles ', '  Laura Anderson  ', '  Rolanda Garza ',
                           'Angela Fox  ', 'Kerl Goldinger '],
             'user_email': ['benjohn@gmail.com', '\\N', '\\N', 'garza.roland@gmail.com', 'nshaiary@aol.com',
                            'k.gold@live.com1'],
             'user_gender': ['Male', 'Male', 'Female', 'Male', 'Female', 'Male']}

    course_enrollment = {'course_id': ['3', '5', '8', '5', '6', '8', '7', '3'],
                         'user_id': ['1001', '1001', '1003', '1003', '1005', '1006', '1001', '1001'],
                         'enrollment_id': ['9010', '9020', '9030', '9040', '9050', '9060', '9070', '9080'],
                         'grade': ['A', '\\N', 'A', '\\N', 'B', 'C', '\\N', 'A'],
                         'department': ['AI  ', 'ML', '  CS', '  DS', '  AI', 'ML', '  CS', 'DS  ']}

    course_df = pd.DataFrame(courses, columns=['course_id', 'course_name', 'course_author', 'course_status',
                                               'course_published_dt'])
    user_df = pd.DataFrame(users, columns=['user_id', 'user_name', 'user_email', 'user_gender'])
    ce_df = pd.DataFrame(course_enrollment, columns=['course_id', 'user_id', 'enrollment_id', 'grade', 'department'])

    course_df.to_csv('/home/itv000558/courses/courses.csv', sep='\u0001', index=False, header=True)
    user_df.to_csv('/home/itv000558/courses/users.csv', sep='\u0001', index=False, header=True)
    ce_df.to_csv('/home/itv000558/courses/course_enrollment.csv', sep='\u0001', index=False, header=True)

    username = getpass.getuser()
    spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        appName(f'{username} | Python - Data Processing - Overview'). \
        master('yarn'). \
        getOrCreate()

    props = cp.ConfigParser()
    props.read("config.ini")
    for each_section in props.sections():
        for (each_key, each_val) in props.items(each_section):
            csv_df = spark.read.csv(f"/user/itv000558/courses/{each_val}.csv", sep='\u0001', header=True)

            for i in csv_df.columns:
                csv_df = csv_df.withColumn(i, when(col(i) == '\\N', None).otherwise(col(i)))

            spark_df = spark.read.table(f"itv000558_courses.{each_val}")

            dtypes_casted = list(
                map(lambda dtype: f' trim(cast({dtype[0]} as {dtype[1]})) as {dtype[0]} ', spark_df.dtypes))

            csv_df = csv_df.selectExpr(dtypes_casted[0:len(dtypes_casted)])
            spark.sql(f"truncate table itv000558_courses.{each_val}")

            csv_df.write.format("hive"). \
                saveAsTable(f"itv000558_courses.{each_val}", mode="append");
            spark.sql(f"select * from itv000558_courses.{each_val}").show(truncate=False)

if __name__ == "__main__":
    main()
