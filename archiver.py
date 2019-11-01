import sys
import getopt
import settings
import psycopg2
import csv
import boto3
import filecmp


def main(argv):
    data_selection_query = ""
    run_type = "dryrun"
    command_options = "archiver.py -q <query> " \
                      "-d <deletion_query> " \
                      "-r <archive|dryrun> " \
                      "-f <filename>"
    try:
        opts, args = getopt.getopt(argv, "hq:d:r:f:",
                                   ["query=", "deletion_query=",
                                    "filename=", "runtype="])
    except getopt.GetoptError:
        print(command_options)
        sys.exit(2)
    for opt, arg in opts:
        if "-h" == opt:
            print(command_options)
            sys.exit()
        elif "-q" == opt:
            data_selection_query = arg
        elif "-d" == opt:
            data_deletion_query = arg
        elif "-f" == opt:
            if arg == "":
                print("Please pass a proper file name")
                print(command_options)
                sys.exit()
            filename = arg
        elif "-r" == opt:
            if arg != "dryrun" and arg != "archive":
                print("-r accepts only archive or dryrun")
                print(command_options)
                sys.exit()
            run_type = arg
        else:
            print(command_options)
            sys.exit()

    if not data_selection_query.upper().startswith("SELECT"):
        print("Provide proper SELECT query for archival data selection")
        sys.exit()
    if "WHERE" not in data_selection_query.upper():
        print("Archive the data in smaller chunks. It is recommended to "
              "add a `where` clause in the select query. It limits the DB "
              "resource utilization")
        sys.exit()

    if not data_deletion_query.upper().startswith("DELETE"):
        print("Provide proper DELETE query for archived data deletion")
        sys.exit()
    if "WHERE" not in data_deletion_query.upper():
        print("Delete the data in smaller chunks. It is recommended to "
              "add a `where` clause in the deletion query. It limits the DB "
              "resource utilization")
        sys.exit()

    delete_query = data_deletion_query.upper().split("FROM", 1)[1]
    select_query = data_selection_query.upper().split("FROM", 1)[1]
    if delete_query != select_query:
        print("Aborting: Data selection and data deletion conditions do not "
              "match")
        sys.exit()
    connection = psycopg2.connect(dbname=settings.DB_NAME,
                                  user=settings.DB_USERNAME,
                                  password=settings.DB_PASSWORD,
                                  host=settings.DB_HOST,
                                  port=settings.DB_PORT)
    cursor = connection.cursor()
    cursor.execute(data_selection_query)
    results = cursor.fetchall()
    csv.register_dialect("with_quotes",
                         quoting=csv.QUOTE_ALL,
                         skipinitialspace=False)
    with open(filename, 'w') as csv_file:
        writer = csv.writer(csv_file, dialect="with_quotes")
        writer.writerows(results)
    csv_file.close()
    region = settings.AWS_REGION
    access_key = settings.AWS_KEY
    secret = settings.AWS_SECRET
    s3_client = boto3.client('s3', aws_access_key_id=access_key,
                             aws_secret_access_key=secret, region_name=region)
    bucket = settings.AWS_S3_BUCKET
    try:
        response = s3_client.upload_file(filename, bucket, filename)
    except Exception as e:
        print(e)
        sys.exit()
    file_name_download = filename.split(".")[0] + '_downloaded.csv'
    with open(file_name_download, 'wb') as download:
        s3_client.download_fileobj(bucket, filename, download)
    match = filecmp.cmp(file_name_download, filename, shallow=False)
    if not match:
        print("Uploaded file and downloaded files do not match")
        sys.exit()
    if run_type != "archive":
        print("All connections working fine. Dry run was successful")
        sys.exit()
    cursor.execute(data_deletion_query)
    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main(sys.argv[1:])
