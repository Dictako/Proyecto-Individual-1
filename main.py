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

@app.get("/") #
def index():
    return str(str(data_juegos.loc[0, 'title']) + ' ' + str(data_items.loc[0, 'user_id']) + ' ' +  str(data_review.loc[0, 'user_id']))

@app.get("/developer")
def developer( desarrollador : str ):
    porcentaje_xaño = {}
    for i in range(0, len(data_juegos)):
        if data_juegos.loc[i, 'developer'] == desarrollador:
            if str(data_juegos.loc[i, 'release_date'])[0:4] in porcentaje_xaño:
                if 'Free' in data_juegos.loc[i, 'price']:
                    porcentaje_xaño[str(data_juegos.loc[i, 'release_date'])[0:4]]['Libre'] += 1
                porcentaje_xaño[str(data_juegos.loc[i, 'release_date'])[0:4]]['Todo'] += 1
            else:
                if 'Free' in data_juegos.loc[i, 'price']:
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
        contenido_libre.append([int(años[año]), todo, str(porcentaje)[0:5] + '%'])
        año += 1
    contenido_libre = pd.DataFrame(contenido_libre, columns= ['Año', 'Cantidad de Items', 'Contenido Libre'])
    return contenido_libre

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
                if not 'Free' in data_juegos.loc[posicion_juego, 'price'] and data_juegos.loc[posicion_juego, 'price'] != ' ':
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


@app.get('/UserForGenre') #
def UserForGenre( genero : str ):
    usuarios = {}
    usuarios_id = []
    horas_años = {}
    lista_años = []
    for i in range (0, len(data_items)):
        posicion_juego = str(data_juegos.loc[data_juegos['id'] == str(data_items.loc[i, 'item_id'])].index)[7:-17]
        if posicion_juego != '':
            posicion_juego = int(posicion_juego)
            if genero in data_juegos.loc[posicion_juego, 'genres']:
                horas = int(data_items.loc[i, 'playtime_forever'])
                año = data_juegos.loc[posicion_juego, 'release_date'][0:4]
                if data_items.loc[i, 'user_id'] not in usuarios_id:
                    usuarios_id.append(data_items.loc[i, 'user_id'])

                if data_items.loc[i, 'user_id'] in usuarios:
                    usuarios[data_items.loc[i, 'user_id']] += horas
                else:
                   usuarios[data_items.loc[i, 'user_id']] = horas
                
                if año != ' ':
                    if año not in lista_años:
                        lista_años.append(año)
                    
                    if año in horas_años:
                        horas_años[año] += horas
                    else:
                        horas_años[año] = horas
    usuario_max = ''
    cantidad_max = 0
    j = 0
    for i in usuarios:
        if cantidad_max < usuarios[i]:
            cantidad_max = usuarios[i]
            usuario_max = usuarios_id[j]
        j += 1
    print('Esta es la lista de cantidad de horas jugadas por cada año para el genero', genero)
    print('Año    Horas')
    j = 0
    for i in horas_años:
        print(lista_años[j],' ', horas_años[i])
        j += 1
    return print('Y el usuario con más horas acumuladas para dicho genero es', usuario_max, 'con un total de', cantidad_max, 'horas.')

@app.get('/best_developer_year')
def best_developer_year( año : int ):
    año = str(año)
    desarrolladores = {}
    nombres_desarroladores = []
    for i in range(0, len(data_review)):
        posicion_juego = str(data_juegos.loc[data_juegos['id'] == str(data_review.loc[i, 'item_id'])].index)[7:-17]
        if posicion_juego != '':
            posicion_juego = int(posicion_juego)
            if data_juegos.loc[posicion_juego, 'release_date'][0:4] == año:
                desarrollador = data_juegos.loc[posicion_juego, 'developer']
                if desarrollador in desarrolladores:
                    if data_review.loc[i, 'sentiment_analysis'] == 2:
                        desarrolladores[desarrollador]['Positivos'] += 1
                else:
                    if desarrollador not in nombres_desarroladores:
                        nombres_desarroladores.append(desarrollador)
                    
                    if data_review.loc[i, 'sentiment_analysis'] == 2:
                        desarrolladores[desarrollador] = {'Positivos': 1}
                    else:
                        desarrolladores[desarrollador]= {'Positivos': 0}
    
    mejores_tres = {'Primero': ['', 0], 'Segundo': ['', 0], 'Tercero': ['', 0]}
    j= 0
    for i in desarrolladores:
        positivos = desarrolladores[i]['Positivos']
        if positivos > mejores_tres['Primero'][1]:
            
            mejores_tres['Tercero'][0] = mejores_tres['Segundo'][0]
            mejores_tres['Tercero'][1] = mejores_tres['Segundo'][1]

            mejores_tres['Segundo'][0] = mejores_tres['Primero'][0]
            mejores_tres['Segundo'][1] = mejores_tres['Primero'][1]
            
            mejores_tres['Primero'][0] = nombres_desarroladores[j]
            mejores_tres['Primero'][1] = positivos

        elif positivos > mejores_tres['Segundo'][1]:
            mejores_tres['Tercero'][0] = mejores_tres['Segundo'][0]
            mejores_tres['Tercero'][1] = mejores_tres['Segundo'][1]

            mejores_tres['Segundo'][0] = nombres_desarroladores[j]
            mejores_tres['Segundo'][1] = positivos

        elif positivos > mejores_tres['Tercero'][1]:
            mejores_tres['Tercero'][0] = nombres_desarroladores[j]
            mejores_tres['Tercero'][1] = positivos
        j += 1
    return mejores_tres



@app.get('/developer_reviews_analysis')
def developer_reviews_analysis( desarrolladora : str ):
    salida = {}
    salida[desarrolladora] = [0, 0]
    for i in range(0, len(data_review)):
        posicion_juego = str(data_juegos.loc[data_juegos['id'] == str(data_review.loc[i, 'item_id'])].index)[7:-17]
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


