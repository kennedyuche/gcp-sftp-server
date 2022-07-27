import click
import logging


@click.command()
@click.option("--input_bucket", prompt="Enter the input GCS bucket name")
@click.option("--input_folder", prompt="Enter the input folder containing the .gz files")
@click.option("--output_bucket", prompt="Enter the GCS bucket the final files will be written into")
@click.option("--output_folder", prompt="Enter the output folder the final files will be written into")
def spark_job(input_bucket, input_folder, output_bucket, output_folder):
    input_path  = "gs://{0}/{1}/*.gz".format(input_bucket, input_folder)
    output_path = "gs://{0}/{1}".format(output_bucket, output_folder)
    
    logging.info("Spark reading files...")
    data = spark.read.csv(input_path, header=True, inferSchema=True, nullValue="null", sep="|")
    data.write.orc(output_path, mode="overwrite")
    logging.info("Spark job completed")


def main():
    logging.info("Starting spark job")
    try:
        spark_job()

        logging.info("Stopping spark session: Exit pyspark shell to start a new spark session")
        spark.stop()
    
    except Exception:
        logging.exception("An error occured: Exiting...")


if __name__ == '__main__':
    main()