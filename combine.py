import pandas as pd
import pendulum
from airflow.decorators import task
import numpy as np
from airflow import DAG
from airflow.models.baseoperator import chain


# define data + engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Column, String, create_engine, Float, Date, Time, select
from sqlalchemy.sql.expression import func

DATABASE_URL = "mysql://root:rootpassword@192.168.110.249:3306/pollutions"
engine = create_engine(DATABASE_URL)
Base = declarative_base()
localSession = Session(bind=engine, autocommit=False, autoflush=False)


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


class PcdDailyMax(Base):
    __tablename__ = "pcd_daily_max"
    station_id = Column(String(10))
    station_id_new = Column(String(100))
    station_name = Column(String(100))
    station_type = Column(String(20))
    latitude = Column(Float)
    longitude = Column(Float)
    date = Column(Date, primary_key=True)
    pm25 = Column(Float)
    pm10 = Column(Float)
    o3 = Column(Float)
    co = Column(Float)
    no2 = Column(Float)
    so2 = Column(Float)
    province = Column(String(50))
    district = Column(String(50))
    subdistrict = Column(String(50))


class GistdaData(Base):
    __tablename__ = "gistda_data"
    province_name = Column(String(50))
    district_name = Column(String(50))
    subdistrict_name = Column(String(50))
    pm25 = Column(Float)
    date = Column(Date)
    time = Column(Time)
    transaction_id = Column(String(100), primary_key=True)


class GistdaDailyMax(Base):
    __tablename__ = "gistda_daily_max"
    province = Column(String(50))
    district = Column(String(50))
    subdistrict = Column(String(50))
    date = Column(Date, primary_key=True)
    pm25 = Column(Float)


@task
def getPCDData():
    yesterday = str(pendulum.today(tz="UTC").date())
    query = (
        select(
            Pollution.station_id,
            Pollution.station_id_new,
            Pollution.station_name,
            Pollution.station_type,
            Pollution.latitude,
            Pollution.longitude,
            Pollution.date,
            Pollution.province,
            Pollution.district,
            Pollution.subdistrict,
            func.max(Pollution.pm25).label("pm25"),
            func.max(Pollution.pm10).label("pm10"),
            func.max(Pollution.o3).label("o3"),
            func.max(Pollution.co).label("co"),
            func.max(Pollution.no2).label("no2"),
            func.max(Pollution.so2).label("so2"),
        )
        .where(Pollution.date == yesterday)
        .group_by(
            Pollution.station_id,
            Pollution.station_id_new,
            Pollution.station_name,
            Pollution.station_type,
            Pollution.latitude,
            Pollution.longitude,
            Pollution.date,
            Pollution.province,
            Pollution.district,
            Pollution.subdistrict,
        )
        .having(func.max(Pollution.pm25) > 0)
    )

    df = pd.read_sql_query(sql=query, con=engine)

    return df


@task
def getGistdaData_max():
    yesterday = str(pendulum.today(tz="UTC").date())
    query = (
        select(
            GistdaData.province_name,
            GistdaData.district_name,
            GistdaData.subdistrict_name,
            GistdaData.date,
            func.max(GistdaData.pm25).label("pm25"),
        )
        .where(GistdaData.date == yesterday)
        .group_by(
            GistdaData.province_name,
            GistdaData.district_name,
            GistdaData.subdistrict_name,
            GistdaData.date,
        )
    )

    df = pd.read_sql_query(sql=query, con=engine)

    return df


@task
def loadSummaryData(data):
    isPCD = len(data.columns) == 16
    if isPCD:
        for i in range(data.shape[0]):
            try:
                newRow = data.iloc[i]
                insertedRow = PcdDailyMax(
                    station_id=newRow["station_id"],
                    station_id_new=newRow["station_id_new"],
                    station_name=newRow["station_name"],
                    station_type=newRow["station_type"],
                    latitude=newRow["latitude"],
                    longitude=newRow["longitude"],
                    date=newRow["date"],
                    pm25=newRow["pm25"],
                    pm10=newRow["pm10"],
                    o3=newRow["pm10"],
                    co=newRow["co"],
                    no2=newRow["no2"],
                    so2=newRow["so2"],
                    province=newRow["province"],
                    district=newRow["district"],
                    subdistrict=newRow["subdistrict"],
                )

                with localSession as session:
                    session.add(insertedRow)
                    session.commit()

            except Exception as e:
                print(e)
            pass
    else:
        for i in range(data.shape[0]):
            try:
                newRow = data.iloc[i]
                insertedRow = GistdaDailyMax(
                    province=newRow["province_name"],
                    district=newRow["district_name"],
                    subdistrict=newRow["subdistrict_name"],
                    date=newRow["date"],
                    pm25=newRow["pm25"],
                )

                with localSession as session:
                    session.add(insertedRow)
                    session.commit()

            except Exception as e:
                print(e)
            pass


@task
def truncate():
    with localSession as session:
        session.execute("""TRUNCATE TABLE pollution""")
        session.execute("""TRUNCATE TABLE gistda_data""")
        session.commit()


with DAG(
    dag_id="summary-daily-max-data",
    start_date=pendulum.datetime(
        2024, 1, 31, 00, 00, tz="Asia/Bangkok"
    ),  ### set at start of dag run
    schedule_interval="30 0 * * *",
    tags=["summary", "max"],
    catchup=False,
) as dag:
    pcd = getPCDData()
    gistda = getGistdaData_max()

    chain(
        [(pcd >> loadSummaryData(pcd)), (gistda >> loadSummaryData(gistda))],
        truncate(),
    )

    # insertNewData(data)
    # avgData = avgPollution(data)
    # saveDatatoCSV(data)
    # loadTransformedData(avgData)
