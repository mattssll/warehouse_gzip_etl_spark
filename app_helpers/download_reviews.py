from methods.download_files import download_file

url = 'https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz'
download_file(url = url, path = "../input_data/metadata.json.gz")