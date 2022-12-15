#imports
from mysql.connector import connect, errorcode, Error
import pandas as pd


#server info (Set these settings per your preferences)
hostname        = "localhost"       #YOUR HOSTNAME ('localhost' = default local)
username        = ""                #YOUR HOST USERNAME
userpw          = ""                #YOUR HOST PASSWORD
databasename    = "ammattihenkilöt" #NAME OF CREATING DB


#csv datan latausmetodi
def lataa_data(tiedostonimi):

    data = pd.read_csv(f'data\\{tiedostonimi}.csv', header=0, sep=';', encoding='latin-1')   
    dataframe = pd.DataFrame(data)
    total_data[tiedostonimi] = len(dataframe)
    return dataframe

#lisääjätesteri
def muokkaa(muutos):
    kursori = ammattihenkilot.cursor()
    try:
        print("suoritetaan päivitystä tauluihin...", end = '')
        kursori.execute(muutos)
        ammattihenkilot.commit()
        kursori.close()
        print("OK")
        return
    except Error as virhe:
        print(f" Virhe: '{virhe.msg}'")
        kursori.close()
        return

#Taulujen lisäys
def lisaa_taulut(taulu):
    #Pöydän nimi muuttujaan dictistä
    table_name = db_tables[taulu]
    #luo pöytä
    try:
        print(f"Luodaan taulu: '{format(taulu)}'...", end='')
        kursori = ammattihenkilot.cursor()
        kursori.execute(table_name)
    #Jos pöytä jo olemassa
    except Error as virhe:
        if virhe.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print(f"taulu on jo olemassa")
        else:
            print(f"{virhe.msg}")
    #pöytä lisätty   
    else:
        print(f"{'OK'}")

def add_nimikkeet():
    
    
    add_column_to_nimike = '''
        ALTER TABLE `nimike`
        ADD Koulutus INT(11);
    '''

    ammattihenkilot = connect(
        host        = hostname,
        user        = username,
        password    = userpw,
        database    = databasename,
        autocommit  = True,
        )
        
    kursori = ammattihenkilot.cursor()
    kursori.execute(add_column_to_nimike)
    ammattihenkilot.commit()
    kursori.close()

    nimike_koulutus_update = '''
        UPDATE nimike SET koulutus = %s WHERE `koodi`= %s;
    '''
    koulutus_arvot = [
        (1, 1), (1, 3), (1, 5), (1, 6), (2, 7), (2, 8), (1,9),
        (1,	10), (1, 12), (2, 14), (1, 16), (1, 20), (1, 21),
        (1,	22), (1, 23), (1, 24), (1, 31), (1, 34), (1, 35),
        (2,	100), (2, 201), (4, 220), (2, 230), (2, 240), (2, 251),
        (2,	252), (3, 253), (3,	254), (3, 255), (3, 266), (2, 300),
        (2,	400), (2,503), (3, 506), (3, 507), (2, 508), (2, 509),
        (2,	600), (3, 602), (3, 603), (3, 604), (3, 605), (3, 607),
        (2,	610), (2, 611), (2, 612), (3, 613), (3, 614), (1,250)
        ]

    kursori = ammattihenkilot.cursor()
    kursori.executemany(nimike_koulutus_update, koulutus_arvot)
    ammattihenkilot.commit()

    ammattihenkilot.close()
    kursori.close()


def add_views():

    view1 = """CREATE VIEW `Yliopistokoulutetut` AS
        SELECT 
            `Ammattioikeus` AS `Yliopistokoulutetut:`
        FROM
            `nimike`
        WHERE
            Koulutus = 1;
    """

    view2 = """ CREATE VIEW `Fyysikot` AS
        SELECT 
            `nimike`.`Ammattioikeus` AS `Ammattioikeus:`,
            SUM(`maarat`.`Ammattioikeuksien_lkm`) AS `lukumäärä vuonna 2021:`
        FROM
            `maarat` JOIN `nimike` ON `maarat`.`Ammattioikeus_koodi` = `nimike`.`Koodi`
        WHERE
            `maarat`.`Vuosi` = 2021 AND `nimike`.`Ammattioikeus` LIKE '%fyysikko%';
    """

    ammattihenkilot = connect(
        host        = hostname,
        user        = username,
        password    = userpw,
        database    = databasename,
        autocommit  = True,
        )

    kursori = ammattihenkilot.cursor()

    kursori.execute(view1)
    kursori.execute(view2)
    ammattihenkilot.commit()

    ammattihenkilot.close()
    kursori.close()


#luodaan datalaskuri
total_data= {}
#luodaan dict pöydille
db_tables = {}

#Taulut
#luodaan nimike-taulu
db_tables['nimike'] = (
    "CREATE TABLE `nimike` ("
    "   `Koodi`             int(11)         NOT NULL,"
    "   `Ammattioikeus`     varchar(150)    ,"
    "   `Ammattioikeus_SV`  varchar(150)    ,"
    "   `Ammattioikeus_EN`  varchar(150)    ,"
    "   PRIMARY KEY (`Koodi`)"
    ") ENGINE = InnoDB")

#Luodaan maarat-taulu
db_tables['maarat'] = (
    "CREATE TABLE `maarat` ("
    "   `Rivi_ID`               int(11)         NOT NULL    AUTO_INCREMENT,"
    "   `Ammattioikeus_koodi`   int(11)         ,"
    "   `Vuosi`                 int(11)         ,"
    "   `Ikä`                   varchar(150)    ,"
    "   `Ammattioikeuksien_lkm` int(11)         ,"
    "   `Ammattihenkilöiden_lkm`int(11)         ,"
    "   PRIMARY KEY (`rivi_ID`)"
    ") ENGINE = InnoDB")

#Luodaan koulutustaso taulu
db_tables['koulutustaso'] = (
    "CREATE TABLE `koulutustaso` ("
    "   `ID`                    int(11)         NOT NULL        AUTO_INCREMENT,"
    "   `Kuvaus`                varchar(100)    ,"
    "   PRIMARY KEY (`ID`)"
    ") ENGINE = InnoDB")

#Luodaan fk välille maarat.ammattioikeus_koodi -> nimike.koodi
add_fk_maarat_nimike = """
    ALTER TABLE `maarat` 
    ADD CONSTRAINT `maarat_nimike_fk`
    FOREIGN KEY (`Ammattioikeus_koodi`)
    REFERENCES `ammattihenkilöt_test`.`nimike` (`Koodi`)
    ON DELETE RESTRICT
    ON UPDATE CASCADE;
"""
#Luodaan fk välille nimike.koulutus - koulutustaso.id
koulutustaso_fk = """
    ALTER TABLE `nimike`
    ADD CONSTRAINT `nimike_koulutustaso_fk`
    FOREIGN KEY (`koulutus`) 
    REFERENCES `koulutustaso` (`id`) 
    ON DELETE RESTRICT 
    ON UPDATE RESTRICT;
"""

#Lisätään uusi sarake nimike tauluun
add_column_to_nimike = """
    ALTER TABLE `nimike`
    ADD Koulutus INT(11);
"""

ammattihenkilot = connect(
        host        = hostname,
        user        = username,
        password    = userpw,
        autocommit  = True,
        )

print("Yhdistetty palvelimeen.")

#luodaan tietokanta
kursori = ammattihenkilot.cursor()
kursori.execute("CREATE DATABASE %s" % databasename)
ammattihenkilot.commit()
kursori.execute("USE %s" % databasename)



#Luodaan taulut tietokantaan
for table in db_tables:
    lisaa_taulut(table)

#lisätään Foreign Keyt
#muokkaa(add_fk_maarat_nimike)


#tallennetaan data csv tiedostoista
nimike_data = lataa_data("nimike")
maarat_data = lataa_data("maarat")
koulutustaso_data = lataa_data("koulutustaso")
#tulostetaan ladattujen rivien määrä
for keys, values in total_data.items():
    print(f"Tiedostosta {keys} ladattu: {values} riviä")

for i,row in nimike_data.iterrows():
            query = "INSERT INTO nimike VALUES (%s,%s,%s,%s);"
            kursori.execute(query, tuple(row))
    
print(f"Yhteensä {i+1} riviä lisätty tauluun 'nimike'")

for i,row in maarat_data.iterrows():

            query2 = "INSERT INTO maarat (`Ammattioikeus_koodi`, `Vuosi`, `Ikä`, `Ammattioikeuksien_lkm`, `Ammattihenkilöiden_lkm`) VALUES (%s,%s,%s,%s,%s);"
            kursori.execute(query2, tuple(row))
print(f"Yhteensä {i+1} riviä lisätty tauluun 'maarat'")

for i,row in koulutustaso_data.iterrows():
            query3 = "INSERT INTO koulutustaso (Kuvaus) VALUES (%s);"
            kursori.execute(query3, tuple(row)[1:])

print(f"Yhteensä {i+1} riviä lisätty tauluun 'koulutustaso'")


ammattihenkilot.close()
kursori.close()
add_nimikkeet()
add_views()

input("Paina ENTER sulkeaksesi terminaalin.")