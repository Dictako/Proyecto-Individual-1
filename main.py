from fastapi import FastAPI
import pandas as pd

app = FastAPI()

data_juegos = pd.read_parquet('Data_Limpia/juegos_steam.parquet')

@app.get("/")
def index():
    return 'jh'