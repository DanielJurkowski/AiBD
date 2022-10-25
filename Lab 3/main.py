import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
from sqlalchemy import create_engine

from typing import Union, List, Tuple

db_string = "postgresql://wbauer_adb:adb2020@pgsql-196447.vipserv.org:5432/wbauer_adb"
db = create_engine(db_string)
connection = db.connect()

def film_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category_id, int):
        return None

    request =   f"""select 
                        film.title, language.name as languge, film_list.category
                    from 
                        film
                    inner join 
                        film_list on film_list.fid = film.film_id
                    inner join 
                        language on language.language_id = film.language_id
                    inner join 
                        film_category on film_category.film_id = film.film_id
                    where 
                        film_category.category_id = {category_id}
                    order by title, language"""

    return pd.read_sql_query(request, con=connection)
    
def number_films_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category_id, int):
        return None

    request =   f"""select 
                        film_list.category, count(film_list.title)
                    from 
                        film_list
                    inner join 
                        category on category.name = film_list.category
                    where
                        category.category_id = {category_id}
                    group by film_list.category
                    """

    return pd.read_sql_query(request, con=connection)

def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(min_length, int) and not isinstance(max_length, int):
        return None

    if max_length < min_length:
        return None

    request = f"""select 
                        film.length, count(film.title)
                    from 
                        film
                    where
                        film.length between {min_length} and {max_length}
                    group by film.length
                    """

    return pd.read_sql_query(request, con=connection)

def client_from_city(city:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(city, str):
        return None

    request =   f"""select 
                        city.city, customer.first_name, customer.last_name
                    from 
                        city
                    inner join 
                        address on address.city_id = city.city_id
                    inner join 
                        customer on customer.address_id = address.address_id
                    where 
                        city.city = '{city}'"""

    return pd.read_sql_query(request, con=connection)

def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389
    
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(length, (int, float)):
        return None

    request = f"""select 
                        film.length, avg(payment.amount)
                    from 
                        film
                    inner join 
                        inventory on inventory.film_id = film.film_id
                    inner join 
                        rental on rental.inventory_id = inventory.inventory_id
                    inner join
                        payment on payment.rental_id = rental.rental_id
                    where
                        film.length = {length}
                    group by film.length"""

    return pd.read_sql_query(request, con=connection)


def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265
    
    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(sum_min, (int, float)):
        return None

    if sum_min < 0:
        return None

    request = f"""select 
                        customer.first_name, customer.last_name, sum(film.length) as sum
                    from 
                        film
                    inner join 
                        inventory on inventory.film_id = film.film_id
                    inner join 
                        rental on rental.inventory_id = inventory.inventory_id
                    inner join
                        customer on customer.customer_id = rental.customer_id
                    group by 
                        customer.first_name, customer.last_name
                    having
                        sum(film.length) > {sum_min}
                    order by
                        sum, customer.last_name, customer.first_name"""

    return pd.read_sql_query(request, con=connection)

def category_statistic_length(name:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(name, str):
        return None


    request = f"""select 
                        category.name as category, avg(film.length), sum(film.length), min(film.length), max(film.length)
                    from 
                        film
                    inner join 
                        film_category on film_category.film_id = film.film_id
                    inner join 
                        category on category.category_id = film_category.category_id
                    where
                        category.name = '{name}'
                    group by 
                        category.name
                    """

    return pd.read_sql_query(request, con=connection)
