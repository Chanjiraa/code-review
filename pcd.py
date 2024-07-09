import requests
import pandas as pd
import pendulum
from datetime import timedelta
import json
from airflow.decorators import task
from airflow import DAG
from airflow.models.baseoperator import chain
import numpy as np

# define data + engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Column, String, create_engine, Float, Date, Time, select
import http.client, urllib

DATABASE_URL = "mysql://root:rootpassword@192.168.110.249:3306/pollutions"
# DATABASE_URL_2 = "mysql://root:rootpassword@192.168.100.79:3306/pollutions"
engine = create_engine(DATABASE_URL)
# envocc_engine = create_engine(DATABASE_URL_2)
Base = declarative_base()
localSession = Session(bind=engine, autocommit=False, autoflush=False)
# envoccSession = Session(bind=envocc_engine, autocommit=False, autoflush=False)


class Pollution(Base):
    __tablename__ = "pollution"
    transaction_id = Column(String(100), primary_key=True)
    station_id = Column(String(10))
    station_name = Column(String(100))
    station_type = Column(String(20))
    latitude = Column(Float)
    longitude = Column(Float)
    date = Column(Date)
    time = Column(Time)
    pm25 = Column(Float)
    pm10 = Column(Float)
    o3 = Column(Float)
    co = Column(Float)
    no2 = Column(Float)
    so2 = Column(Float)
    province = Column(String(100))
    district = Column(String(100))
    subdistrict = Column(String(100))
    station_id_new = Column(String(100))


# Utility function
def checkUnique(id):
    with localSession as session:
        query = select(Pollution.transaction_id)
        result = session.execute(query).unique()
    isUnique = id not in result

    return isUnique


def notifyPushover(message: str):
    """
    This function send message to PushOver app in my phone
    """
    conn = http.client.HTTPSConnection("api.pushover.net:443")
    conn.request(
        "POST",
        "/1/messages.json",
        urllib.parse.urlencode(
            {
                "token": "anxk29bo3bdywn9p88u1bqsrqkkezg",
                "user": "utpoz63nbbwk5tudhsz1k8xyc147fd",
                "message": message,
            }
        ),
        {"Content-type": "application/x-www-form-urlencoded"},
    )
    conn.getresponse()


# @task()
# def checkConnection():
#     """
#     Check connectivity of API
#     """
#     url = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
#     response_code = requests.get(url).status_code
#     if response_code != 200:
#         notifyPushover("!!API connection Problem!!")
#     else:
#         pass


@task()
def getDataviaAPI():
    url = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
    response = requests.get(url)

    if response.status_code == 200:
        response = response.json()
        data = pd.DataFrame(response["stations"])
        data = data.join(pd.json_normalize(data["AQILast"])).drop("AQILast", axis=1)
        return data
    else:
        notifyPushover("!!API connection Problem!!")


@task()
def transformData(data):
    # exclude columns
    excluded = list(
        set(
            [i for i in data.columns if i.endswith("color_id")]
            + [i for i in data.columns if i.startswith("AQI")]
            + [i for i in data.columns if i.endswith("aqi")]
            + ["nameEN", "areaEN", "forecast"]
        )
    )
    data.drop(columns=excluded, inplace=True)

    # stationID >> station_id
    data.rename(columns={"stationID": "station_id"}, inplace=True)

    # province cleaning
    data["province"] = data["areaTH"].str.split(", ")
    data["areaTH_subprov"] = data["province"].apply(lambda x: x[0])
    data["province"] = data["province"].apply(lambda x: x[1] if len(x) == 2 else None)
    data.loc[data["province"].isna(), "province"] = "กรุงเทพฯ"
    # ! แก้ไขแล้ว ?? เช็ค สงขลา ด้วยadd clear "จ."
    # data = data.join(data["province"].str.extract(r"([^จ.].*)"))
    # data["province"] = data[0]
    # data.drop(columns=0, inplace=True)

    # create amp, tambol
    data = data.join(data["areaTH_subprov"].str.extract(r"(เขต\S*)|(อ\.\S*)"))
    data.rename(columns={0: "District", 1: "District2"}, inplace=True)
    data["District"].fillna(data["District2"], inplace=True)
    data.drop(columns="District2", inplace=True)

    data = data.join(data["areaTH_subprov"].str.extract(r"(แขวง\S*)|(ต\.\S*)"))
    data.rename(columns={0: "Subdistrict", 1: "Subdistrict2"}, inplace=True)
    data["Subdistrict"].fillna(data["Subdistrict2"], inplace=True)
    data.drop(columns=["Subdistrict2", "areaTH", "areaTH_subprov"], inplace=True)

    data = data.loc[
        data["time"].str.contains("|".join(["07:00", "12:00", "18:00", "23:00"]))
    ]

    # create primary key
    data.loc[:, "transaction_id"] = (
        data["station_id"] + data["date"] + data["time"] + data["lat"] + data["long"]
    )

    # change column name
    data.rename(
        columns={
            "nameTH": "station_name",
            "stationType": "station_type",
            "PM25.value": "pm25",
            "PM10.value": "pm10",
            "O3.value": "o3",
            "CO.value": "co",
            "NO2.value": "no2",
            "SO2.value": "so2",
            "District": "district",
            "Subdistrict": "subdistrict",
        },
        inplace=True,
    )

    data.loc[:, "province"] = data["province"].str.strip()
    data.loc[:, "district"] = data["district"].str.strip()
    data.loc[:, "subdistrict"] = data["subdistrict"].str.strip()
    data.loc[:, "station_name"] = data["station_name"].str.strip()

    # * Hard code data correction

    data.loc[data["station_id"] == "bkp56t", "district"] = "เขตดินแดง"
    data.loc[data["station_id"] == "bkp58t", "district"] = "เขตราษฎร์บูรณะ"
    data.loc[data["station_id"] == "03t", "subdistrict"] = "แขวงแสมดำ"
    data.loc[data["station_id"] == "50t", "subdistrict"] = "แขวงปทุมวัน"
    data.loc[data["station_id"] == "52t", "subdistrict"] = "แขวงบางยี่เรือ"
    data.loc[data["station_id"] == "53t", "subdistrict"] = "แขวงสะพานสอง"
    data.loc[data["station_id"] == "54t", "subdistrict"] = "แขวงดินแดง"
    data.loc[data["station_id"] == "bkp101t", "subdistrict"] = "แขวงบางชัน"
    data.loc[data["station_id"] == "bkp102t", "subdistrict"] = "แขวงบางมด"
    data.loc[data["station_id"] == "bkp103t", "subdistrict"] = "แขวงบางอ้อ"
    data.loc[data["station_id"] == "bkp104t", "subdistrict"] = "แขวงบางแค"
    data.loc[data["station_id"] == "bkp105t", "subdistrict"] = "แขวงแสมดำ"
    data.loc[data["station_id"] == "bkp110t", "district"] = "เขตดุสิต"
    data.loc[data["station_id"] == "bkp110t", "subdistrict"] = "แขวงสวนจิตรดา"
    data.loc[data["station_id"] == "bkp112t", "subdistrict"] = "แขวงบางคอแหลม"
    data.loc[data["station_id"] == "bkp114t", "subdistrict"] = "แขวงวังใหม่"
    data.loc[data["station_id"] == "bkp115t", "subdistrict"] = "แขวงลาดยาว"
    data.loc[data["station_id"] == "bkp116t", "subdistrict"] = "แขวงจตุจักร"
    data.loc[data["station_id"] == "bkp117t", "subdistrict"] = "แขวงหนองบอน"
    data.loc[data["station_id"] == "bkp118t", "subdistrict"] = "แขวงคลองสองต้นนุ่น"
    data.loc[data["station_id"] == "bkp119t", "subdistrict"] = "แขวงคลองตัน"
    data.loc[data["station_id"] == "bkp120t", "subdistrict"] = "แขวงดอนเมือง"
    data.loc[data["station_id"] == "bkp121t", "subdistrict"] = "แขวงบางมด"
    data.loc[data["station_id"] == "bkp122t", "subdistrict"] = "แขวงคลองเตย"
    data.loc[data["station_id"] == "bkp123t", "subdistrict"] = "แขวงจตุจักร"
    data.loc[data["station_id"] == "bkp124t", "subdistrict"] = "แขวงบางขุนนนท์"
    data.loc[data["station_id"] == "bkp125t", "subdistrict"] = "แขวงคลองกุ่ม"
    data.loc[data["station_id"] == "bkp126t", "subdistrict"] = "แขวงหลักสอง"
    data.loc[data["station_id"] == "bkp127t", "subdistrict"] = "แขวงทวีวัฒนา"
    data.loc[data["station_id"] == "bkp128t", "subdistrict"] = "แขวงลาดกระบัง"
    data.loc[data["station_id"] == "bkp129t", "subdistrict"] = "แขวงกระทุ่มราย"
    data.loc[data["station_id"] == "bkp130t", "subdistrict"] = "แขวงอนุสาวรีย์"
    data.loc[data["station_id"] == "bkp131t", "subdistrict"] = "แขวงบางโคล่"
    data.loc[data["station_id"] == "bkp56t", "subdistrict"] = "แขวงดินแดง"
    data.loc[data["station_id"] == "bkp57t", "subdistrict"] = "แขวงพระโขนงใต้"
    data.loc[data["station_id"] == "bkp58t", "subdistrict"] = "แขวงราษฎร์บูรณะ"
    data.loc[data["station_id"] == "bkp59t", "subdistrict"] = "แขวงทุ่งพญาไท"
    data.loc[data["station_id"] == "bkp60t", "subdistrict"] = "แขวงดุสิต"
    data.loc[data["station_id"] == "bkp61t", "subdistrict"] = "แขวงวัดโสมนัส"
    data.loc[data["station_id"] == "bkp62t", "subdistrict"] = "แขวงสัมพันธวงศ์"
    data.loc[data["station_id"] == "bkp63t", "subdistrict"] = "แขวงสามเสนใน"
    data.loc[data["station_id"] == "bkp64t", "subdistrict"] = "แขวงพลับพลา"
    data.loc[data["station_id"] == "bkp65t", "subdistrict"] = "แขวงวังใหม่"
    data.loc[data["station_id"] == "bkp66t", "subdistrict"] = "แขวงสีลม"
    data.loc[data["station_id"] == "bkp69t", "subdistrict"] = "แขวงบางโพงพาง"
    data.loc[data["station_id"] == "bkp70t", "subdistrict"] = "แขวงคลองตันเหนือ"
    data.loc[data["station_id"] == "bkp71t", "subdistrict"] = "แขวงพัฒนาการ"
    data.loc[data["station_id"] == "bkp72t", "subdistrict"] = "แขวงบางนาเหนือ"
    data.loc[data["station_id"] == "bkp73t", "subdistrict"] = "แขวงลาดยาว"
    data.loc[data["station_id"] == "bkp74t", "subdistrict"] = "แขวงดอนเมือง"
    data.loc[data["station_id"] == "bkp75t", "subdistrict"] = "แขวงออเงิน"
    data.loc[data["station_id"] == "bkp76t", "subdistrict"] = "แขวงคลองจั่น"
    data.loc[data["station_id"] == "bkp77t", "subdistrict"] = "แขวงคันนายาว"
    data.loc[data["station_id"] == "bkp78t", "subdistrict"] = "แขวงลาดกระบัง"
    data.loc[data["station_id"] == "bkp79t", "subdistrict"] = "แขวงมีนบุรี"
    data.loc[data["station_id"] == "bkp80t", "subdistrict"] = "แขวงกระทุ่มราย"
    data.loc[data["station_id"] == "bkp81t", "subdistrict"] = "แขวงหนองบอน"
    data.loc[data["station_id"] == "bkp82t", "subdistrict"] = "แขวงดาวคะนอง"
    data.loc[data["station_id"] == "bkp83t", "subdistrict"] = "แขวงคลองต้นไทร"
    data.loc[data["station_id"] == "bkp84t", "subdistrict"] = "แขวงวัดท่าพระ"
    data.loc[data["station_id"] == "bkp85t", "subdistrict"] = "แขวงศิริราช"
    data.loc[data["station_id"] == "bkp86t", "subdistrict"] = "แขวงฉิมพลี"
    data.loc[data["station_id"] == "bkp87t", "subdistrict"] = "แขวงทวีวัฒนา"
    data.loc[data["station_id"] == "bkp88t", "subdistrict"] = "แขวงบางหว้า"
    data.loc[data["station_id"] == "bkp89t", "subdistrict"] = "แขวงหนองแขม"
    data.loc[data["station_id"] == "bkp90t", "subdistrict"] = "แขวงบางบอนใต้"
    data.loc[data["station_id"] == "bkp91t", "subdistrict"] = "แขวงบางมด"
    data.loc[data["station_id"] == "bkp92t", "subdistrict"] = "แขวงวัดสามพระยา"
    data.loc[data["station_id"] == "bkp93t", "subdistrict"] = "แขวงห้วยขวาง"
    data.loc[data["station_id"] == "bkp96t", "subdistrict"] = "แขวงลาดพร้าว"
    data.loc[data["station_id"] == "bkp98t", "subdistrict"] = "แขวงอนุสาวรีย์"
    data.loc[data["station_id"] == "bkp99t", "subdistrict"] = "แขวงสะพานสูง"
    data.loc[data["station_id"] == "05t", "subdistrict"] = "แขวงบางนาใต้"
    data.loc[data["station_id"] == "109t", "subdistrict"] = "ต.ในเมือง"
    data.loc[data["station_id"] == "bkp133t", "subdistrict"] = "แขวงบางพลัด"
    data.loc[data["station_id"] == "110t", "province"] = "อำนาจเจริญ"
    data.loc[data["station_id"] == "110t", "district"] = "อ.เมือง"
    data.loc[data["station_id"] == "110t", "subdistrict"] = "ต.บุ่ง"
    data.loc[data["station_id"] == "111t", "province"] = "สุรินทร์"
    data.loc[data["station_id"] == "111t", "district"] = "อ.เมือง"
    data.loc[data["station_id"] == "111t", "subdistrict"] = "ต.นอกเมือง"
    data.loc[data["station_id"] == "o70", "subdistrict"] = "ต.สุเทพ"
    data.loc[data["station_id"] == "o70", "district"] = "อ.เมือง"
    data.loc[data["station_id"] == "o70", "province"] = "เชียงใหม่"
    data.loc[data["station_id"] == "o71", "subdistrict"] = "ต.หางดง"
    data.loc[data["station_id"] == "o71", "district"] = "อ.ฮอด"
    data.loc[data["station_id"] == "o71", "province"] = "เชียงใหม่"
    data.loc[data["province"] == "ประจวบคิรีขันธ์", "province"] = "ประจวบคีรีขันธ์"
    data.loc[data["province"] == "จ.เชียงใหม่", "province"] = "เชียงใหม่"
    data.loc[data["province"] == "จ.บึงกาฬ", "province"] = "บึงกาฬ"
    data.loc[data["province"] == "จ.บึงกาฬ ", "province"] = "บึงกาฬ"
    data.loc[data["province"] == "จ.อำนาจเจริญ", "province"] = "อำนาจเจริญ"
    data.loc[data["province"] == "จ.สุรินทร์", "province"] = "สุรินทร์"
    data.loc[data["province"] == "กรุงเทพฯ", "province"] = "กรุงเทพมหานคร"
    data.loc[data["province"] == "กาฬสินธ์ุ", "province"] = "กาฬสินธุ์"
    data.loc[data["station_id"] == "107t", "province"] = "กาฬสินธุ์"

    data["district"] = data["district"].str.removeprefix("เขต")
    data["district"] = data["district"].str.removeprefix("อ.")
    data["subdistrict"] = data["subdistrict"].str.removeprefix("แขวง")
    data["subdistrict"] = data["subdistrict"].str.removeprefix("ต.")
    data["province"] = data["province"].str.removeprefix("จ.")
    data.loc[:, "station_id_new"] = data["station_id"] + data["province"]

    data["district"] = np.where(
        data["district"] == "เมือง", "เมือง" + data["province"], data["district"]
    )

    return data


@task()
def loadTransformedData(data, session):
    for i in range(data.shape[0]):
        try:
            newRow = data.iloc[i]

            insertedRow = Pollution(
                transaction_id=newRow["transaction_id"],
                station_id=newRow["station_id"],
                station_name=newRow["station_name"],
                station_type=newRow["station_type"],
                latitude=newRow["lat"],
                longitude=newRow["long"],
                date=newRow["date"],
                time=newRow["time"],
                pm25=float(newRow["pm25"]),
                pm10=float(newRow["pm10"]),
                o3=float(newRow["pm10"]),
                co=float(newRow["co"]),
                no2=float(newRow["no2"]),
                so2=float(newRow["so2"]),
                province=newRow["province"],
                district=newRow["district"],
                subdistrict=newRow["subdistrict"],
                station_id_new=newRow["station_id_new"],
            )

            with session as session:
                session.add(insertedRow)
                session.commit()
        except Exception as e:
            print(e)
            pass


# @task()
# def saveDatatoCSV(data):
#     data.to_csv("/opt/airflow/temp_data/exp_data.csv", index=False)


CURRENT_STATION = {
    "02t",
    "03t",
    "05t",
    "08t",
    "100t",
    "101t",
    "102t",
    "103t",
    "104t",
    "105t",
    "106t",
    "107t",
    "108t",
    "109t",
    "10t",
    "110t",
    "111t",
    "112t",
    "11t",
    "12t",
    "13t",
    "14t",
    "16t",
    "17t",
    "18t",
    "19t",
    "20t",
    "21t",
    "22t",
    "24t",
    "25t",
    "26t",
    "27t",
    "28t",
    "29t",
    "30t",
    "31t",
    "32t",
    "33t",
    "34t",
    "35t",
    "36t",
    "37t",
    "38t",
    "39t",
    "40t",
    "41t",
    "42t",
    "43t",
    "44t",
    "46t",
    "47t",
    "50t",
    "52t",
    "53t",
    "54t",
    "57t",
    "58t",
    "59t",
    "60t",
    "61t",
    "62t",
    "63t",
    "67t",
    "68t",
    "69t",
    "70t",
    "71t",
    "72t",
    "73t",
    "74t",
    "75t",
    "76t",
    "77t",
    "78t",
    "79t",
    "80t",
    "81t",
    "82t",
    "83t",
    "84t",
    "85t",
    "86t",
    "87t",
    "88t",
    "89t",
    "90t",
    "91t",
    "92t",
    "93t",
    "94t",
    "95t",
    "96t",
    "97t",
    "98t",
    "99t",
    "bkp100t",
    "bkp101t",
    "bkp102t",
    "bkp103t",
    "bkp104t",
    "bkp105t",
    "bkp110t",
    "bkp112t",
    "bkp114t",
    "bkp116t",
    "bkp117t",
    "bkp118t",
    "bkp119t",
    "bkp120t",
    "bkp121t",
    "bkp122t",
    "bkp123t",
    "bkp124t",
    "bkp125t",
    "bkp126t",
    "bkp127t",
    "bkp128t",
    "bkp129t",
    "bkp130t",
    "bkp131t",
    "bkp133t",
    "bkp56t",
    "bkp57t",
    "bkp58t",
    "bkp59t",
    "bkp60t",
    "bkp61t",
    "bkp62t",
    "bkp63t",
    "bkp64t",
    "bkp65t",
    "bkp66t",
    "bkp67t",
    "bkp69t",
    "bkp70t",
    "bkp71t",
    "bkp72t",
    "bkp73t",
    "bkp74t",
    "bkp75t",
    "bkp76t",
    "bkp77t",
    "bkp78t",
    "bkp79t",
    "bkp80t",
    "bkp81t",
    "bkp82t",
    "bkp83t",
    "bkp84t",
    "bkp85t",
    "bkp86t",
    "bkp87t",
    "bkp88t",
    "bkp89t",
    "bkp90t",
    "bkp91t",
    "bkp92t",
    "bkp93t",
    "bkp94t",
    "bkp95t",
    "bkp96t",
    "bkp97t",
    "bkp98t",
    "bkp99t",
    "o10",
    "o21",
    "o22",
    "o30",
    "o31",
    "o34",
    "o38",
    "o61",
    "o65",
    "o66",
    "o67",
    "o68",
    "o69",
    "o70",
    "o71",
    "o72",
    "o26",
    "o27",
    "o73",
    "o28",
    "o29",
    "bkp115t",
    "o24",
    "o23",
    "o59",
    "o74",
}


@task()
def checkUniqueStationID():
    with localSession as session:
        result = set(
            session.execute("SELECT DISTINCT station_id FROM pollution").scalars().all()
        )
        new_station = list(result - CURRENT_STATION)

    if len(new_station) > 0:
        notifyPushover(f"New Station Added!!: {new_station}")
    else:
        print("!!!!!No new station!!!!!")

    # data = getDataviaAPI()
    # transformedData = transformData(data)
    # loadTransformedData(transformedData)
    # checkUniqueStationID()
    # # saveDatatoCSV(transformedData)


with DAG(
    dag_id="get_pollution_data_daily_avg",
    start_date=pendulum.datetime(2023, 7, 24, 14, 00, tz="Asia/Bangkok"),
    schedule_interval="20 7,12,18,23 * * *",
    tags=["pm", "pollution"],
    catchup=False,
) as dag:
    data = getDataviaAPI()
    transformedData = transformData(data)

    chain(
        # checkConnection(),
        data,
        transformedData,
        [
            loadTransformedData(transformedData, localSession),
            # loadTransformedData(transformedData, envoccSession),
        ],
        checkUniqueStationID(),
        # saveDatatoCSV(transformedData),
    )
