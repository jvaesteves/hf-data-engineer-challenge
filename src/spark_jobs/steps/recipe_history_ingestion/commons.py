from pyspark.sql.functions import coalesce, regexp_extract, lit
from pyspark.sql import Column

def cast_pt_time_to_minutes_integer(column: Column) -> Column:
    hours = coalesce(regexp_extract(column, r'(\d+)H', 1).cast('int'), lit(0))
    minutes = coalesce(regexp_extract(column, r'(\d+)M', 1).cast('int'), lit(0))
    return hours * 60 + minutes