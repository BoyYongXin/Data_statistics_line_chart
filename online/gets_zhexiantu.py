import os
from fastapi import FastAPI
from fastapi.responses import FileResponse

app = FastAPI()
app.debug=True
@app.get("/data_report/")
def read_root():
    some_file_path = "crawl_platform_email.html"
    return FileResponse(some_file_path)
@app.get("/data_report_7/")
def read_root_7():
    some_file_path = "crawl_platform_email_7.html"
    return FileResponse(some_file_path)
if __name__ == '__main__':
    command = "uvicorn gets_zhexiantu:app --host 0.0.0.0 --port 7881 --reload"
    os.system(command)
