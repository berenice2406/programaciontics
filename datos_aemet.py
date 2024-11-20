import json
import pandas as pd 
import requests
import dtale

def get_datos_json(url: str):
    headers ={'accept':'application.json'}

    r=requests.get(url,headers=headers)
    if r.status_code == 200:
        cuerpo=json.loads(r.text)
    else:
        cuerpo=None
    return r.status_code, cuerpo

def getAEMETTABLE(*, url=None, file_name=None) -> pd.DataFrame:
    tabla_resultado = None
    # if (url is None and not file_name is None) or (not url is None and file_name is None):
    if any([(url is None and not file_name is None), (not url is None and file_name is None)]):
        if not url is None:
            status_code, primeros_datos = get_datos_json(url)
            if status_code == 200:
                status_code, datos = get_datos_json(primeros_datos['datos'])
                if status_code == 200:
                    return pd.DataFrame(datos)
        if not file_name is None:
            tabla_resultado = pd.read_csv(file_name)
   
    return tabla_resultado


def main():
    tabla_pandas= getAEMETTABLE(file_name='todas-observaciones.csv')

    if not tabla_pandas is None:
        print(f'los datos tienen {len(tabla_pandas)} tuplas')
        print(f'hay {len(tabla_pandas["idema"].drop_duplicates())} estaciones meteorològicas')
        #Obtener la tabla de los idema y ubicaciones de las estaciones en las que no ha habido nieve en algùn momento
        tabla_resultado=tabla_pandas.query('prec > 0')[['idema','ubi']].drop_duplicates()
        print(tabla_resultado)
        print(f'los datos tienen {len(tabla_pandas)} tuplas')

        # d=dtale.show(tabla_pandas, host='localhost',open_browser=True)
        # d.open_browser()
        # input('pulsa para continuar')

         #Obtener la tabla de los idema y ubicaciones de las estaciones en las que ha habido nieve en algùn momento
        tabla_resultado=tabla_pandas.query('nieve > 0')[['idema','ubi', 'alt']].drop_duplicates()
        print(tabla_resultado)
        print(f'los datos tienen {len(tabla_resultado)} tuplas')

        tabla_resultado.to_csv('estaciones_con_nieve.csv', index=False)


        tabla_provincias: pd.DataFrame=getAEMETTABLE(file_name='datos-idema-provincia.csv')
        tabla_idema_provincia=tabla_resultado.merge(tabla_provincias, on='idema')
        print(tabla_idema_provincia)


         #Obtener la tabla de los idema y ubicaciones de las estaciones en las que no ha habido nieve en algùn momento
        idemas_con_nieve=set(tabla_resultado['idema'].to_list())
        todos_los_idemas=set(tabla_pandas["idema"].drop_duplicates().to_list())
        idemas_sin_nieve=pd.DataFrame(todos_los_idemas.difference(idemas_con_nieve))
        print(idemas_sin_nieve)
        print(f'los datos tienen {len(idemas_sin_nieve)} tuplas')

        #Obtener la tabla de los idema y ubicaciones de las estaciones 
        #por debajo de los 1500 metros en las que ha habido nieve en algùn momento
        altura_maxima=1500
        tabla_resultado=tabla_pandas.query('`nieve` > 0 and alt < @altura_maxima')[['idema','ubi']].drop_duplicates()
        print(tabla_resultado)
        print(f'tabla_resultado tiene {len(tabla_resultado)} tuplas')



def old_main():
    url="http://opendata.aemet.es/opendata/api/observacion/convencional/todas"

    status_code, primeros_datos=get_datos_json(url)
    if status_code==200:
        status_code, datos=get_datos_json(primeros_datos['datos'])
        if status_code==200:
            #print(datos)
            #print(type(datos))
            tabla_pandas=pd.DataFrame(datos)
            print(tabla_pandas)
            d=dtale.show(tabla_pandas, host='localhost',open_browser=True)
            #d.open_browser()
            input('pulsa para continuar')

            #tabla_lluvia=tabla_pandas.query('prec > 0')
            tabla_resultado=tabla_pandas.query('prec > 0')[['idema','ubi']].drop_duplicates
            print(tabla_resultado)
        else:
            print('error al leer los datos finales')
    else:
        print('error al leer los datos finales')


if __name__=="__main__":
    main()