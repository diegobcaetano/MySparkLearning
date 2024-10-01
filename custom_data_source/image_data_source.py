from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from PIL import Image

schema = StructType([
    StructField("image_path", StringType()),
    StructField("camera_make", StringType()),
    StructField("date_time_original", StringType()),
])


class EXIFReader:
    def __init__(self):
        pass

    @classmethod
    def name(cls):
        return "exif"

    def read(self, spark, path):
        def scan():
            for image_path in spark.sparkContext.wholeTextFiles(path)[0][1].split("\n"):
                with Image.open(image_path) as img:
                    exif_data = img._getexif()
                    if exif_data is not None:
                        row = (
                            image_path,
                            exif_data.get(306, '').printable,  # Image Make
                            exif_data.get(36867, '').printable,  # DateTimeOriginal
                            # ... outros campos
                        )
                        yield row
                    else:
                        yield (image_path, None, None)  # Caso não tenha metadados EXIF

        rdd = spark.sparkContext.parallelize(scan())
        return spark.createDataFrame(rdd, schema)


spark = SparkSession.builder.appName("EXIFReader").config("spark.sql.extensions", "org.apache.spark.sql.exif.EXIFReader").getOrCreate()
spark.dataSource.register(EXIFReader)

BASE_PATH_DF = "../../data/images"
df = spark.read.format("exif").load(BASE_PATH_DF)

df.show()
