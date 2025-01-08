from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import explode, col, concat_ws, when

def create_spark_session():
    return SparkSession.builder \
        .appName("QRDA XML Parser") \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0") \
        .getOrCreate()

def create_qrda_schema():
    observation_schema = StructType([
        StructField("code", StructType([
            StructField("_code", StringType(), True)
        ]), True),
        StructField("statusCode", StructType([
            StructField("_code", StringType(), True)
        ]), True),
        StructField("effectiveTime", StructType([
            StructField("_value", StringType(), True)
        ]), True),
        StructField("value", StructType([
            StructField("_value", StringType(), True),
            StructField("_unit", StringType(), True)
        ]), True)
    ])

    patient_schema = StructType([
        StructField("name", StructType([
            StructField("given", StringType(), True),
            StructField("family", StringType(), True)
        ]), True),
        StructField("administrativeGenderCode", StructType([
            StructField("_code", StringType(), True)
        ]), True),
        StructField("birthTime", StructType([
            StructField("_value", StringType(), True)
        ]), True),
        StructField("observation", observation_schema, True)  # For multi-patient format
    ])

    return StructType([
        # For single patient format
        StructField("recordTarget", StructType([
            StructField("patientRole", StructType([
                StructField("id", StructType([
                    StructField("_root", StringType(), True)
                ]), True),
                StructField("patient", patient_schema, True)
            ]), True)
        ]), True),
        # For both formats
        StructField("component", StructType([
            StructField("structuredBody", StructType([
                StructField("component", StructType([
                    StructField("section", StructType([
                        StructField("entry", ArrayType(StructType([
                            # For multi-patient format
                            StructField("patientRole", StructType([
                                StructField("id", StructType([
                                    StructField("_root", StringType(), True)
                                ]), True),
                                StructField("patient", patient_schema, True)
                            ]), True),
                            # For single patient format
                            StructField("observation", observation_schema, True)
                        ]), True), True)
                    ]), True)
                ]), True)
            ]), True)
        ]), True)
    ])

def parse_qrda_xml(spark, xml_path):
    try:
        # Read XML with schema
        df = spark.read \
            .format("xml") \
            .option("rowTag", "ClinicalDocument") \
            .schema(create_qrda_schema()) \
            .load(xml_path)

        # Check if it's a multi-patient document
        entries_df = df.select(
            explode("component.structuredBody.component.section.entry").alias("entry")
        )
        
        is_multi_patient = entries_df.select("entry.patientRole").first()[0] is not None

        if is_multi_patient:
            # Process multi-patient format
            result_df = entries_df.select(
                col("entry.patientRole.id._root").alias("patient_id"),
                col("entry.patientRole.patient.name.given").alias("first_name"),
                col("entry.patientRole.patient.name.family").alias("last_name"),
                col("entry.patientRole.patient.birthTime._value").alias("birth_date"),
                col("entry.patientRole.patient.administrativeGenderCode._code").alias("gender"),
                col("entry.patientRole.patient.observation.code._code").alias("observation_code"),
                col("entry.patientRole.patient.observation.effectiveTime._value").alias("observation_date"),
                col("entry.patientRole.patient.observation.value._value").alias("measurement"),
                col("entry.patientRole.patient.observation.value._unit").alias("unit")
            )
        else:
            # Process single patient format
            patient_info = df.select(
                col("recordTarget.patientRole.id._root").alias("patient_id"),
                col("recordTarget.patientRole.patient.name.given").alias("first_name"),
                col("recordTarget.patientRole.patient.name.family").alias("last_name"),
                col("recordTarget.patientRole.patient.birthTime._value").alias("birth_date"),
                col("recordTarget.patientRole.patient.administrativeGenderCode._code").alias("gender")
            )

            observations = entries_df.select(
                col("entry.observation.code._code").alias("observation_code"),
                col("entry.observation.effectiveTime._value").alias("observation_date"),
                col("entry.observation.value._value").alias("measurement"),
                col("entry.observation.value._unit").alias("unit")
            )

            result_df = patient_info.crossJoin(observations)

        return result_df

    except Exception as e:
        print(f"Error parsing XML: {str(e)}")
        raise

def format_results(df):
    """Add some basic formatting and derived columns"""
    return df.withColumn(
        "full_name", 
        concat_ws(" ", col("first_name"), col("last_name"))
    ).withColumn(
        "birth_date_formatted", 
        concat_ws("-", 
            col("birth_date").substr(1, 4),
            col("birth_date").substr(5, 2),
            col("birth_date").substr(7, 2)
        )
    )

def main():
    spark = create_spark_session()
    
    try:
        # Process single patient file
        print("\nProcessing single patient file:")
        single_patient_df = parse_qrda_xml(spark, "sample_qrda.xml")
        formatted_single = format_results(single_patient_df)
        formatted_single.select(
            "full_name", "gender", "birth_date_formatted", "measurement", "unit"
        ).show(truncate=False)
        
        # Process multi-patient file
        print("\nProcessing multi-patient file:")
        multi_patient_df = parse_qrda_xml(spark, "sample_qrda_multi.xml")
        formatted_multi = format_results(multi_patient_df)
        formatted_multi.select(
            "full_name", "gender", "birth_date_formatted", "measurement", "unit"
        ).show(truncate=False)
        
        # Show statistics for multi-patient file
        print("\nPatient Statistics (Multi-patient file):")
        formatted_multi.groupBy("gender").count().show()
        formatted_multi.select("measurement").summary().show()
        
    except Exception as e:
        print(f"Error in main: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()