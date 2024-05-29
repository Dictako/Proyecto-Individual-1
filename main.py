import pandas as pd
from fastapi import FastAPI
import numpy as np
import os
import pyarrow as pa
import pyarrow.parquet as pq

app = FastAPI()

data_items = pd.read_parquet('Data_Limpia/items_combinados.parquet')
data_juegos = pd.read_parquet('Data_Limpia/juegos_steam.parquet')
data_review = pd.read_parquet('Data_Limpia/nuevo_reviews_limpio.parquet')

@app.get("/")
def index():
    return 'Bienvenido, para tener una mejor experiencia con esta API ingrese al siguiente link: https://proyecto-individual-1-d6mc.onrender.com/docs#/'

@app.get("/developer")
def developer( desarrollador : str ):
    desarrollador = str(desarrollador)
    porcentaje_xaño = {}
    for i in range(0, len(data_juegos)):
        if data_juegos.loc[i, 'developer'] == desarrollador:
            if str(data_juegos.loc[i, 'release_date'])[0:4] in porcentaje_xaño:
                if 'Free' in str(data_juegos.loc[i, 'price']):
                    porcentaje_xaño[str(data_juegos.loc[i, 'release_date'])[0:4]]['Libre'] += 1
                porcentaje_xaño[str(data_juegos.loc[i, 'release_date'])[0:4]]['Todo'] += 1
            else:
                if 'Free' in str(data_juegos.loc[i, 'price']):
                    porcentaje_xaño[str(data_juegos.loc[i, 'release_date'])[0:4]] = {'Libre': 1, 'Todo':1}
                else:
                    porcentaje_xaño[str(data_juegos.loc[i, 'release_date'])[0:4]] = {'Libre': 0, 'Todo':1}
    años = pd.DataFrame(porcentaje_xaño)
    años = años.columns
    contenido_libre = []
    año = 0
    for i in porcentaje_xaño:
        todo = porcentaje_xaño[i]['Todo']
        porcentaje = porcentaje_xaño[i]['Libre']*100/porcentaje_xaño[i]['Todo']
        contenido_libre.append({'Año':int(años[año]), 'Cantidad de items': todo, 'Contenido libre': str(porcentaje)[0:5] + '%'})
        año += 1
    
    salida = ''
    for i in range(0,len(contenido_libre)):
        salida += str(contenido_libre[i])
    return salida

@app.get('/userdata')
def userdata( User_id : str ):
    User_id = str(User_id)
    dinero = 0
    items = 0
    buscador = True
    Finalizador = True
    i = 0
    while buscador:
        if data_items.loc[i, 'user_id'] == User_id:
            if Finalizador:
                Finalizador = False
            items += 1
            posicion_juego = str(data_juegos.loc[data_juegos['id'] == str(data_items.loc[i, 'item_id'])].index)[7:-17]
            if posicion_juego != '':
                posicion_juego = int(posicion_juego)
                if not 'Free' in data_juegos.loc[posicion_juego, 'price'] and data_juegos.loc[posicion_juego, 'price'] != ' ' and data_juegos.loc[posicion_juego, 'price'] != 'Third-party':
                    dinero += float(data_juegos.loc[posicion_juego, 'price'])
            
        if data_items.loc[i+1, 'user_id'] != User_id and not Finalizador:
            buscador = False
                
        i += 1
    i = 0
    buscador = True
    Finalizador = True
    reviews = 0
    while buscador:
        if data_review.loc[i, 'user_id'] == User_id:
            if Finalizador:
                Finalizador = False
            reviews += 1 
        if data_items.loc[i+1, 'user_id'] != User_id and not Finalizador:
            buscador = False
                
        i += 1
    porcentaje = reviews*100/items
    return ('El usuario ' + str(User_id) + ' gastó un total de '+ str(dinero) + '$, su porcentaje de recomendaciones es del ' + str(porcentaje) + '% y tiene ' + str(items) + ' items en su biblioteca.')


@app.get('/UserForGenre')
#def UserForGenre( genero : str ): Debe devolver el usuario que acumula más 
#horas jugadas para el género dado y una lista de la acumulación de horas jugadas por año de lanzamiento.
#def UserForGenre( genero : str ): Debe devolver el usuario que acumula más 
#horas jugadas para el género dado y una lista de la acumulación de horas jugadas por año de lanzamiento.
def UserForGenre( genero : str ):
    juegos = data_juegos[data_juegos['genres'].str.contains(genero, case=False, na=False)]
    juegos = juegos.drop(columns=['Unnamed: 0', 'publisher', 'genres', 'app_name', 'title', 'url', 'tags', 'reviews_url', 'specs', 'price', 'early_access', 'developer'])
    juegos['release_date'] = juegos['release_date'].str[:4]
    juegos.rename(columns={'id': 'item_id'}, inplace=True)
    jugadores = pd.merge(data_items, juegos, on='item_id')
    jugadores['playtime_forever'] = jugadores['playtime_forever'].astype(int)

    años = jugadores.drop(columns=['user_id', 'item_id'])
    años = años.groupby('release_date').agg({'playtime_forever': 'sum'})
    
    jugadores = jugadores.drop(columns=['item_id', 'release_date'])
    jugadores = jugadores.groupby('user_id').agg({'playtime_forever': 'sum'})

    jugadores = jugadores.sort_values(by='playtime_forever', ascending=False).head(5)
    
    año_salida = []
    for i in range (0, len(años)):
        año_salida.append(años.iloc[i])
        

    return str(str(jugadores) + str(año_salida))

@app.get('/best_developer_year')
def best_developer_year( año : int ):
    juegos = data_juegos.copy()
    juegos['release_date'] = juegos['release_date'].str[:4]
    juegos = juegos[juegos['release_date'].str.contains(str(año), case=False, na=False)]
    juegos = juegos.drop(columns=['Unnamed: 0', 'publisher', 'app_name' ,'genres', 'title', 'url', 'tags', 'reviews_url', 'specs', 'price', 'early_access'])
    juegos.rename(columns={'id': 'item_id'}, inplace=True)

    review = data_review.copy()
    review = review.drop(columns=['Unnamed: 0', 'user_id', 'user_url', 'funny', 'posted', 'last_edited', 'helpful', 'recommend'])
    review['item_id'] = review['item_id'].astype(str)
    
    review = pd.merge(review, juegos, on='item_id')
    review = review.drop(columns=['item_id', 'release_date'])
    review = review.groupby('developer').agg({'sentiment_analysis': 'sum'})
    review['sentiment_analysis'] = round(review['sentiment_analysis']/2)
    review = review.sort_values(by='sentiment_analysis', ascending=False).head(3)

    return str(review)

@app.get('/developer_reviews_analysis')
def developer_reviews_analysis( desarrolladora : str ):
    juegos = data_juegos.copy()
    juegos = juegos[juegos['developer'].str.contains(desarrolladora, case=False, na=False)]
    juegos = juegos.drop(columns=['Unnamed: 0', 'publisher', 'app_name' ,'genres', 'title', 'url', 'tags', 'reviews_url', 'specs', 'price', 'early_access'])
    juegos.rename(columns={'id': 'item_id'}, inplace=True)
    
    review = data_review.copy()
    review = review.drop(columns=['Unnamed: 0', 'user_id', 'user_url', 'funny', 'posted', 'last_edited', 'helpful', 'recommend'])
    review['item_id'] = review['item_id'].astype(str)
    review = pd.merge(review, juegos, on='item_id')
    
    salida = {}
    salida[desarrolladora] = [0, 0]
    
    for i in range(0, len(review)):
        posicion_juego = str(data_juegos.loc[data_juegos['id'] == str(review.loc[i, 'item_id'])].index)[7:-17]
        if posicion_juego != '':
            posicion_juego = int(posicion_juego)
            if desarrolladora == data_juegos.loc[posicion_juego, 'developer']:
                if data_review.loc[i, 'sentiment_analysis'] == 2:
                    salida[desarrolladora][1] += 1
                elif data_review.loc[i, 'sentiment_analysis'] == 0:
                    salida[desarrolladora][0] += 1


    return salida

@app.get('/recomendacion_juego')
def recomendacion_juego(producto : str):
    posicion_juego = str(data_juegos.loc[data_juegos['id'] == str(producto)].index)[7:-17]
    juego_semilla = data_juegos.iloc[int(posicion_juego)]
    precio = juego_semilla['price']
    generos_aux = juego_semilla['genres']
    generos = []
    genero = ''
    fecha = juego_semilla['release_date'][0:4]

    for i in range(1, len(generos_aux)):
        letra = generos_aux[i]
        if letra == ',' or letra == ']':
            generos.append(genero)
            genero = ''
        elif letra == ' ':
            pass
        else:
            genero += letra
    
    recomendaciones = []
    desarrolladores = []
    for j in range(0, len(generos)):
        for i in range(0, len(data_juegos)):
            juego = data_juegos.iloc[i]
            juego_año = juego['release_date'][0:4]
            if generos[j] in str(juego['genres']):
                if juego['id'] != juego_semilla['id'] and juego_año == fecha:
                    if juego['price'] == precio and not juego['early_access']:
                        if juego['id'] not in recomendaciones and juego['developer'] not in desarrolladores:
                            recomendaciones.append(juego['id'])
                            desarrolladores.append(juego['developer'])
    i = 0
    bandera = True
    while bandera:
        juego = data_juegos.iloc[i]
        if len(recomendaciones) == 5:
            bandera = False
        elif juego['price'] == precio and juego['id'] not in recomendaciones:
            recomendaciones.append(juego['id'])
        i += 1
    recomendaciones_final = []
    for i in range(0, len(recomendaciones)):
        posicion_juego = int(str(data_juegos.loc[data_juegos['id'] == str(recomendaciones[i])].index)[7:-17])
    
        juego = data_juegos.loc[posicion_juego, 'title']
        recomendaciones_final.append(juego)
    return recomendaciones_final


