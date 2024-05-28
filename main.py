from fastapi import FastAPI
import pandas as pd

app = FastAPI()

data_items = pd.read_parquet('C:\\Users\\Alañ\\Documents\\Henry\\Proyecto 1\\Proyecto-Individual-1\\Data Limpia\\items_combinados.parquet')

@app.get("/")
def index():
    return data_items.iloc[0]