import urllib.request

print('Beginning file download with urllib2...')


def download_file(url, path):
    """
    This function is called by airflow to download the input files from an url
    """
    try:
        print("started downloading file")
        urllib.request.urlretrieve(url, path)
        print("file finished downloading successfully")
        return {"response_code" : "200"}
    except Exception as e:
        raise ValueError("download failed", e)

