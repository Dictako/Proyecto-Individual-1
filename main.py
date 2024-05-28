from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def index():
    return 'Hola, este es mi proyecto'

@app.get("/developer")
def developer( desarrollador : str ):
    import pandas as pd
    
    data_juegos = pd.read_csv('https://github.com//Dictako//Proyecto-Individual-1//blob//main//Data%20Limpia//juegos_steam.csv')

    porcentaje_xaño = {}
    for i in range(0, len(data_juegos)):
        if data_juegos.loc[i, 'developer'] == desarrollador:
            if data_juegos.loc[i, 'release_date'][0:4] in porcentaje_xaño:
                if 'Free' in data_juegos.loc[i, 'price']:
                    porcentaje_xaño[data_juegos.loc[i, 'release_date'][0:4]]['Libre'] += 1
                porcentaje_xaño[data_juegos.loc[i, 'release_date'][0:4]]['Todo'] += 1
            else:
                if 'Free' in data_juegos.loc[i, 'price']:
                    porcentaje_xaño[data_juegos.loc[i, 'release_date'][0:4]] = {'Libre': 1, 'Todo':1}
                else:
                    porcentaje_xaño[data_juegos.loc[i, 'release_date'][0:4]] = {'Libre': 0, 'Todo':1}
    años = pd.DataFrame(porcentaje_xaño)
    años = años.columns
    contenido_libre = []
    año = 0
    for i in porcentaje_xaño:
        todo = porcentaje_xaño[i]['Todo']
        porcentaje = porcentaje_xaño[i]['Libre']*100/porcentaje_xaño[i]['Todo']
        contenido_libre.append([int(años[año]), todo, str(porcentaje)[0:5] + '%'])
        año += 1
    contenido_libre = pd.DataFrame(contenido_libre, columns= ['Año', 'Cantidad de Items', 'Contenido Libre'])
    return contenido_libre