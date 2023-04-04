import time
start_time = time.time()
# import ray
# ray.init(runtime_env={'env_vars': {'__MODIN_AUTOIMPORT_PANDAS__': '1'}})
#import modin.pandas as pd

# ^ teoretycznie mialo dzialac szybciej z tym ale dziala wolniej, nie wiem czemu

import pandas as pd
from scipy import stats
import datetime
from astral import LocationInfo
from astral.sun import sun
import geopandas as gpd
import datetime
import pytz
import concurrent.futures
import pymongo
from neo4j import GraphDatabase
utc=pytz.UTC
import redis

from io import StringIO
# stations = geopandasowy dataframe z numerami i geometria stacji
# df= dataframe z wynikami pomiarowymi, numerem stacji i czasem

class loading_date:

    def row_seter(self, row, ast, string):
        s = sun(ast.observer, date=datetime.date(int(string[0:4]), int(string[5:7]), int(string[8:10])))



        our_date = datetime.datetime(int(string[0:4]), int(string[5:7]), int(string[8:10]),
                                     int(string[-5:-3]),int(string[14:17])  )

        our_date = our_date.replace(tzinfo=utc)
        dusk = s["dusk"].replace(tzinfo=utc)
        dawn = s["dawn"].replace(tzinfo=utc)


        if our_date >= dawn and our_date <= dusk:
            # nie modyfikuje istnijacego df'a
            row["isDay"] = True
        else:
            row["isDay"] = False


        #print(row)

        return row

    def isDay(self, stations):
        # zwraca nam stacje z jej geometria
        # poprawka, w df sa WSZYSTKIE stacje...
        #list_of_stations = stations["ifcid"] #liczenie dla wszystkich stacji
        list_of_stations = [249190890] #to jako konkretna stacja


        # w razie co tutaj obrobic by sprawdzalo wiele/wybrana stacje
        for s in list_of_stations:
            point = stations.loc[stations["ifcid"] == s]
            #print(point.crs)
            point = point.to_crs(4326)
            x, y = point['geometry'].x, point['geometry'].y #to jest fi lambda, do petli w 89 kolmnu x,y
            #print(x,y)

            ast = LocationInfo("stacja_pogodowa", "Poland", "Europe/Warsaw", x, y)
            print("liczenie stacji: ",s)

# próby wprowadzenia wielowątkowości

            # with concurrent.futures.ProcessPoolExecutor() as executor:
            # #     results = [executor.submit(self.row_seter,
            # #                                # tutaj dorobic tak, by bralo tylko te fragmenty dataframa ktore dotyczna danej stacji
            # #                                # bo teraz liczy dla wszystkich po kilka razy...
            # #                                df.loc[df[0] == s], row, ast, row[2]) for df in self.dataset]
            #
            #     try:
            #             result = executor.map()
            #         for df in self.dataset:
            #             future = executor.submit(self.row_seter(), lambda row: row, ast, lambda row: row["data"])
            #
            #
            #     except TimeoutError:
            #             print('Waited too long')


                #print(df.loc[df[0] == s])
            for df in self.dataset:
                #tylko tutaj result naprawic i powinno dzialac!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                # to dziala!!!
                df.loc[df["id"] == s] = df.loc[df["id"] == s].apply(lambda row: self.row_seter(row, ast, row["data"]), axis=1) #jak aktualizowac poszczegolne rekordy w DS, dla wierszy nie kolumn

    def load_from_net(self):
        import requests, tarfile, io

        # https://dane.imgw.pl/datastore/getfiledown/Arch/Telemetria/Hydro/2019/Hydro_2019-07.tar
        file = requests.get("https://dane.imgw.pl/datastore/getfiledown/Arch/Telemetria/Hydro/"+str(self.year)+
        "/Hydro_"+str(self.year)+"-"+str(self.month)+".tar")
        tar = tarfile.TarFile(io.BytesIO(file.content))
        # TypeError: expected str, bytes or os.PathLike object, not BytesIO ^
        tar.extractall(r"C:\2")

        #print("pobralo sie")

    def __init__(self, year, month):
        self.year = year
        self.month = month

        self.df_pow_woj = None
        #WCZYTYWANIE
        #self.load_from_net()
        # nie dziala, ale chyba nie potrzebne


        # dorobic zera przy miesiacach jednocyfrowych
        self.df_300 = pd.read_csv(r'dane\B00300S_' + str(year) + "_"+str(month)+".csv",
                                  on_bad_lines='skip',header=None, sep=';', low_memory=False, decimal=",",
                                  names=["id", "dan", "data", "wartosci", "isDay"]
                                  )
        self.df_305 = pd.read_csv(r'dane\B00305A_' + str(year) + "_"+str(month)+".csv",
                                  on_bad_lines='skip',header=None, sep=';', low_memory=False, decimal=",",
                                  names=["id", "dan", "data", "wartosci", "isDay"])
        self.df_202 = pd.read_csv(r'dane\B00202A_' + str(year) + "_"+str(month)+".csv",
                                  on_bad_lines='skip',header=None, sep=';', low_memory=False, decimal=",",
                                  names=["id", "dan", "data", "wartosci", "isDay"])
        self.df_702 = pd.read_csv(r'dane\B00702A_' + str(year) + "_"+str(month)+".csv",
                                  on_bad_lines='skip',header=None, sep=';', low_memory=False, decimal=",",
                                  names=["id", "dan", "data", "wartosci", "isDay"])
        self.df_703 = pd.read_csv(r'dane\B00703A_' + str(year) + "_"+str(month)+".csv",
                                  on_bad_lines='skip',header=None, sep=';', low_memory=False, decimal=",",
                                  names=["id", "dan", "data", "wartosci", "isDay"])
        # u mnie w danych ten plik jest pusty, najlepiej dorobic ify sprawdzajace czy plik jest pusty, albo wyjatek
        #self.df_608 = pd.read_csv(r'dane\B00608S_' + str(year) + "_"+str(month)+".csv", on_bad_lines='skip')
        self.df_604 = pd.read_csv(r'dane\B00604S_' + str(year) + "_"+str(month)+".csv",
                                  on_bad_lines='skip',header=None, sep=';', low_memory=False, decimal=",",
                                  names=["id", "dan", "data", "wartosci", "isDay"])
        self.df_606 = pd.read_csv(r'dane\B00606S_' + str(year) + "_"+str(month)+".csv",
                                  on_bad_lines='skip',header=None, sep=';', low_memory=False, decimal=",",
                                  names=["id", "dan", "data", "wartosci", "isDay"])
        self.df_802 = pd.read_csv(r'dane\B00802A_' + str(year) + "_"+str(month)+".csv",
                                  on_bad_lines='skip',header=None, sep=';', low_memory=False, decimal=",",
                                  names=["id", "dan", "data", "wartosci", "isDay"])
        self.df_714 = pd.read_csv(r'dane\B00714A_' + str(year) + "_"+str(month)+".csv",
                                  on_bad_lines='skip',header=None, sep=';', low_memory=False, decimal=",",
                                  names=["id", "dan", "data", "wartosci", "isDay"])
        self.df_910 = pd.read_csv(r'dane\B00910A_' + str(year) + "_"+str(month)+".csv",
                                  on_bad_lines='skip',header=None, sep=';', low_memory=False, decimal=",",
                                  names=["id", "dan", "data", "wartosci", "isDay"])

        #pozbyc sie kolumny indeks 4

        self.dataset = [self.df_300, self.df_305, self.df_202, self.df_702, self.df_703, #self.df_608,
                        self.df_604, self.df_606, self.df_802, self.df_714, self.df_910]

        # #usuwanie pustej kolumny
        for d in self.dataset:
            d["our_date"] = pd.to_datetime(d["data"]).dt.date



    def finding_woj_and_pow(self, shp, stations, szukane):
        our_area = shp.loc[shp['name'] == szukane]
        our_area = our_area.set_crs(2180, allow_override=True)




        points_in_polygon = gpd.sjoin(stations,our_area)
        #print(points_in_polygon.to_string())

        #teraz wybrac wiersze ze stations ktor maja ifcid takie jak z points in polygon i im do kolumny wojewodztwo przypisać
        # wartosc szukane

        points_in_polygon.sort_index(inplace=True)

        stations.loc[stations["ifcid"].isin(points_in_polygon["ifcid"]), "wojewodztwo"] = szukane




        #print(stations.loc[stations["wojewodztwo"] == szukane])




    # dorobic podzial na dzien i noc na dni
    def calculate_median(self):
        print("policzone mediany: ")
        for d in self.dataset:
            print(d[3].median())

    def calculate_mean(self):
        print("policzone srednie: ")
        for d in self.dataset:
            print(d[3].mean())

    def calculate_trim_mean(self):
        print("policzone srednie obcinanej: ")
        for d in self.dataset:
            print(stats.trim_mean(d[3], 0.1))
    def finding_woj_and_pow(self, shp, stations):
        shp = shp.set_crs(2180, allow_override=True)
        points_in_polygon = gpd.sjoin(stations, shp, rsuffix="shp", lsuffix="stacji")



        return points_in_polygon[["ifcid", "name_shp"]]
        #print(points_in_polygon.columns)

    def statystyki_lab5(self, stations):
        srednie = []
        mediany = []
        sr_obcinane = []

        for my_df1 in test.dataset:
            # print(my_df1[(my_df1["id"] == 249190890)].to_string())
            Srednia = my_df1[["id", "our_date", "wartosci", "isDay"]][(my_df1["id"] == 249190890)].groupby(
                ["id", "our_date", "isDay"]).mean()
            srednie.append(Srednia)
            Mediana = my_df1[["id", "our_date", "wartosci", "isDay"]][(my_df1["id"] == 249190890)].groupby(
                ["id", "our_date", "isDay"]).median()
            mediany.append(Mediana)
            my_df2 = my_df1[["id", "our_date", "wartosci", "isDay"]][(my_df1["id"] == 249190890)].copy()
            Srednia_obcinana = my_df2.groupby(["id", "our_date", "isDay"]).aggregate(stats.trim_mean, .15)
            sr_obcinane.append(Srednia_obcinana)

        return srednie, mediany, sr_obcinane


    def statystyki_lab6(self, stations, woj, pow):
        df_woj = test.finding_woj_and_pow(woj, stations).rename(columns={"ifcid": "id", "name_shp": "nazwa_woj"})
        df_pow = test.finding_woj_and_pow(pow, stations).rename(columns={"ifcid": "id", "name_shp": "nazwa_pow"})
        df_pow_woj = pd.merge(left=df_woj, right=df_pow, how="inner")

        self.df_pow_woj = df_pow_woj

        srednie = []
        mediany = []

        for my_df1 in test.dataset:
            tmp = pd.merge(left=my_df1, right=df_pow_woj, how="inner")
            # print(my_df1[(my_df1["id"] == 249190890)].to_string())
            Srednia = tmp[["id", "our_date", "wartosci", "isDay", "nazwa_woj", "nazwa_pow"]][
                (tmp["id"] == 249190890)].groupby(
                ["id", "our_date", "isDay", "nazwa_woj", "nazwa_pow"]).mean()
            srednie.append(Srednia)
            Mediana = tmp[["id", "our_date", "wartosci", "isDay", "nazwa_woj", "nazwa_pow"]][
                (tmp["id"] == 249190890)].groupby(
                ["id", "our_date", "isDay", "nazwa_woj", "nazwa_pow"]).median()
            mediany.append(Mediana)

        return srednie, mediany
    def mongoDB_upload(self):
        connection = pymongo.MongoClient("mongodb+srv://PagDB:PAG2022@pagdb.zunxebz.mongodb.net/")

        #stworzenie bazy danych o nazwie PagMongoDB
        db =  connection["PagMongoDB"]

        # w bazie danych zrob kolekcje (plik) o nazwie B00300S i dodaj tam df'a
        db.B00300S.insert_many(self.df_300.to_dict(orient="record"))

        print("koniec uploadu")
        connection.close()

    def mongoDB_downlad(self):
        connection = pymongo.MongoClient("mongodb+srv://PagBD:PAG2022@pagbd.zunxebz.mongodb.net/")
        db = connection["PagMongoDB"]

        names_list = ["B00300s", "B00305A"]
        df_list = []

        for s in names_list:
            mycollection = db[s]
            all_records = mycollection.find()
            list_cursor = list(all_records)
            df = pd.DataFrame(list_cursor)
            df_list.append(df)
            print(df)

    def redis_upload(self, df):
        db = redis.Redis(
            host='redis-11254.c293.eu-central-1-1.ec2.cloud.redislabs.com',
            port=11254,
            password='I9HguuLHhentENw6t2NfE2wdWrdZroTX')

        i = 0
        for index, row in df.iterrows():
            db.set(index,      str(row["id"])+";"+str(row["dan"])+";"+str(row["data"])+";"+str(row["wartosci"])+";"+
                           str(row["isDay"])+";")
            print(index)
            i = i+1
            if i >= 100:
                break

        print("Finished")

    def redis_download(self, list):
        db = redis.Redis(
            host='redis-11254.c293.eu-central-1-1.ec2.cloud.redislabs.com',
            port=11254,
            password='I9HguuLHhentENw6t2NfE2wdWrdZroTX')

        df = pd.DataFrame()
        for i in list:
            StringData = StringIO(db.get(i).decode())

            df_temp = pd.read_csv(StringData, sep=";", header=None,
                                  names=["id", "dan", "data", "wartosci", "isDay"])

            df = pd.concat([df, df_temp], axis=0, ignore_index=True)

        return df

    def redis_upload_hastable(self, df):
        db = redis.Redis(
            host='redis-11254.c293.eu-central-1-1.ec2.cloud.redislabs.com',
            port=11254,
            password='I9HguuLHhentENw6t2NfE2wdWrdZroTX')

        i = 0

        for index, row in df.iterrows():
            db.hset(str(row["id"]), str(row["data"]), str(row["wartosci"]))
            print(index)
            i = i+1
            if i >= 10:
                break

        print("Uploaded")

    def redis_download_hastable(self, key):
        db = redis.Redis(
            host='redis-11254.c293.eu-central-1-1.ec2.cloud.redislabs.com',
            port=11254,
            password='I9HguuLHhentENw6t2NfE2wdWrdZroTX')

        return db.hgetall(key)




    def neo4j_upload_pow_i_woj_i_stations(self, driver, woj, pow, stations):
        # zrobic jeden raz upload do neo4j woj i pow
        session = driver.session()
        woj.to_csv(
            "C:\\Users\\janek\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-53308171-3c9b-4f77-9b7e-a9e3c4ce39ff\\import\\woj.csv")

        pow.to_csv(
            "C:\\Users\\janek\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-53308171-3c9b-4f77-9b7e-a9e3c4ce39ff\\import\\pow.csv")

        stations.to_csv(
            "C:\\Users\\janek\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-53308171-3c9b-4f77-9b7e-a9e3c4ce39ff\\import\\stations.csv")

        result = session.run(
            "LOAD CSV WITH HEADERS FROM 'file:///woj.csv'  AS row WITH row MERGE (w:Wojewodztwo{id:row.id, name:row.name, upper_leve:row.upper_leve })")
        result = session.run(
            "LOAD CSV WITH HEADERS FROM 'file:///pow.csv'  AS row WITH row MERGE (p:Powiat {id:row.id, name:row.name, upper_leve:row.upper_leve, id_upper_leve:row.id_upper_l })")
        # result = session.run("LOAD CSV WITH HEADERS FROM 'file:///stations.csv'  AS row WITH row MERGE (s:Station {id:row.ifcid, })")

        result = session.run("MATCH (p:Powiat), (w:Wojewodztwo) WHERE p.id_upper_leve = w.id CREATE (w)-[r:JEST_W]->(p)")

        session.close()

    def neo4j_upload(self, driver, df):

        small_df = pd.merge(left = df, right = self.df_pow_woj , how="left")
        small_df.to_csv(
            "C:\\Users\\janek\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-53308171-3c9b-4f77-9b7e-a9e3c4ce39ff\\import\\out.csv")

        session = driver.session()
        # delete all
        result = session.run("MATCH (n) DETACH DELETE n")

        result = session.run(
            "LOAD CSV WITH HEADERS FROM 'file:///out.csv'  AS row WITH row MERGE (r:Record {id:row.id, dan:row.dan, data:row.data, wartosci:row.wartosci, our_date:row.our_date, nazwa_pow:row.nazwa_pow })")


        session.close()

    def neo4j_relationships(self, driver):
        session = driver.session()
        result = session.run("MATCH (r:Record), (p:Powiat) WHERE r.nazwa_pow = p.name CREATE (p)-[:ZROBIONO_W]->(r)")

        session.close()

    def neo4j_download(self, driver):
        session = driver.session()
        result = session.run("MATCH (r:Record) WHERE r.nazwa_pow = 'Koszalin' AND r.data = '2022-01-03 02:00' RETURN r.wartosci;")
        #result = session.run("MATCH (r:Record) WHERE r.wartosci = 11 RETURN r.nazwa_pow;")

        print("wynik zapytania: ")
        print(result.single()[0])

        session.close()



# testy
if __name__ == '__main__':


    test = loading_date(2022, '01')

    # test mongoDB
    test.mongoDB_upload()

    # test redis
    test.redis_upload(test.df_714)

    test.redis_download([1,2,3])

    # test statystyk
    stations = gpd.read_file(r'dane\Dane\effacility.geojson')
    woj = gpd.read_file(r'dane\Dane\woj.shp')
    pow = gpd.read_file(r'dane\Dane\powiaty.shp')
    data_framy = ["df_300", "df_305", "df_202", "df_702", "df_703", "df_604", "df_606", "df_802", "df_714", "df_910"]

    test.isDay(stations)

    tmp = test.statystyki_lab6(stations, woj, pow)
    srednie = tmp[0]
    mediany = tmp[1]

    tmp = "Srednie:"
    print(f"{tmp:#^40}")
    for i in range(len(data_framy)):
        print(f"{data_framy[i]:.^20}")
        print(srednie[i].head(4))
    tmp = "Mediany:"
    print(f"{tmp:#^40}")
    for i in range(len(data_framy)):
        print(f"{data_framy[i]:.^20}")
        print(mediany[i].head(4))

    # test neo4j
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "haslo"))

    test.neo4j_upload(driver, test.df_714)
    test.neo4j_upload_pow_i_woj_i_stations(driver, woj, pow, stations)
    test.neo4j_relationships(driver)

    test.neo4j_download(driver)