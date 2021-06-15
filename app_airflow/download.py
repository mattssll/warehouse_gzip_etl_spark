import wget

def download_file(url, path):
    try:
        print("started downloading file")
        wget.download(url, path)
        print("file finished downloading successfully")
        return {"response_code" : 200}
    except Exception as e:
        raise ValueError("download failed", e)

result = download_file(url = 'https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz', path='./input_data/metadata.json.gz')
