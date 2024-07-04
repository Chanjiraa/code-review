from airflow import DAG
import pandas as pd
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

# define data + engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Column, String, create_engine, Float, Date, Time, select

DATABASE_URL = "mysql://root:rootpassword@192.168.110.249:3306/pollutions"
DATABASE_URL_2 = "mysql://root:rootpassword@192.168.100.79:3306/pollutions"
engine = create_engine(DATABASE_URL)
envocc_engine = create_engine(DATABASE_URL_2)
Base = declarative_base()
localSession = Session(bind=engine, autocommit=False, autoflush=False)
envoccSession = Session(bind=envocc_engine, autocommit=False, autoflush=False)


class GistdaData(Base):
    __tablename__ = "gistda_data"
    province_name = Column(String(50))
    district_name = Column(String(50))
    subdistrict_name = Column(String(50))
    pm25 = Column(Float)
    date = Column(Date)
    time = Column(Time)
    transaction_id = Column(String(100), primary_key=True)


# Utility function
def checkUnique(id):
    with localSession as session:
        query = select(GistdaData.transaction_id)
        result = session.execute(query).unique()
    isUnique = id not in result

    return isUnique


###


@task()
def getRawData():
    today = str(pendulum.now(tz="Asia/Bangkok").date())
    hours = str(pendulum.now(tz="Asia/Bangkok").strftime("%H:00"))
    url = f"https://pm25.gistda.or.th/rest/getPM25byTambonAsCSV?dt={today}%20{hours}"
    data = pd.read_csv(url)

    return data


@task()
def transformData(data):
    data.rename(
        columns={
            "pv_tn": "province_name",
            " ap_tn": "district_name",
            " tb_tn": "subdistrict_name",
            " datetime": "datetime",
            " pm25": "pm25",
        },
        inplace=True,
    )

    data["transaction_id"] = (
        data["province_name"]
        + data["district_name"]
        + data["subdistrict_name"]
        + data["datetime"]
    )

    data["date"] = data["datetime"].apply(lambda x: x.split(" ")[0])
    data["time"] = data["datetime"].apply(lambda x: x.split(" ")[1])

    data.drop(columns="datetime", inplace=True)

    return data


@task()
def loadTransformedData(data, session):
    for i in range(data.shape[0]):
        try:
            newRow = data.iloc[i]
            insertedRow = GistdaData(
                province_name=newRow["province_name"],
                district_name=newRow["district_name"],
                subdistrict_name=newRow["subdistrict_name"],
                pm25=newRow["pm25"],
                date=newRow["date"],
                time=newRow["time"],
                transaction_id=newRow["transaction_id"],
            )

            with session as session:
                session.add(insertedRow)
                session.commit()
        except Exception as e:
            print(e)
            pass


with DAG(
    dag_id="get-gistda-data",
    start_date=pendulum.datetime(2023, 7, 24, 14, 00, tz="Asia/Bangkok"),
    schedule_interval="50 * * * *",
    tags=["pm", "gistda"],
    catchup=False,
) as dag:
    data = getRawData()
    transformed = transformData(data)
    chain(
        data,
        transformed,
        [
            loadTransformedData(transformed, localSession),
            # loadTransformedData(transformed, envoccSession),
        ],
    )
