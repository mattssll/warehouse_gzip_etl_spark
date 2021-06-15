import wget

def download_file(url: str, path:str) -> None:
    try:
        print("started downloading file")
        wget.download(url, path)
        print("file finished downloading successfully")
        return {"response_code" : 200}
    except Exception as e:
        raise ValueError("download failed", e)