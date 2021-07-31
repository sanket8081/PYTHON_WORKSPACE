import datetime
import requests
import tabula
import os
import mysql.connector

pdf_date = datetime.datetime.now() - datetime.timedelta(days=2)
formatedDate = pdf_date.strftime("%d" + "th" + "%B%G");
creation_Date = pdf_date.strftime("%Y/%m/%d");
url = "https://www.mohfw.gov.in/pdf/CummulativeCovidVaccinationReport" + formatedDate.lower() + ".pdf"
path = "C:/SanketData/PYTHON_WORKSPACE/VaccinationsPDF/" + formatedDate + ".pdf"
newPath = "C:/SanketData/PYTHON_WORKSPACE/VaccinationsPDF/" + "abc" + ".pdf"


def fileDownload():
    req = requests.get(url, stream=True)
    with open(path, 'wb') as fd:
        for chunk in req.iter_content(10000):
            fd.write(chunk)
    print("File downloaded from :" + url)


def extractTablesFromPDF():
    global url, path, formatedDate, creation_Date
    if os.path.exists(path):
        os.remove(path)
    formatedDate = pdf_date.strftime("%d" + "%B%G");
    url = "https://www.mohfw.gov.in/pdf/CummulativeCovidVaccinationReport" + formatedDate.lower() + ".pdf"
    path = "C:/SanketData/PYTHON_WORKSPACE/VaccinationsPDF/" + formatedDate + ".pdf"
    fileDownload()
    df = tabula.read_pdf(path, pages="all")

    dataDF = df[1]
    dataDF = dataDF.rename(columns={"Unnamed: 0": "2nd Dose"})
    dataDF = dataDF.rename(columns={"Unnamed: 1": "Total Doses"})

    stateName = dataDF['State/UT'].tolist()
    doseSecond = dataDF['2nd Dose'].tolist()
    doseTotal = dataDF['Total Doses'].tolist()

    finalList = []
    for x, y, z in zip(stateName[1:], doseSecond[1:], doseTotal[1:]):
        tempList = [creation_Date, x, int(z.replace(',', '')) - int(y.replace(',', '')), int(y.replace(',', '')),
                    int(z.replace(',', ''))]
        finalList.append(tempList)
        # print(x,y,z)
    # print(finalList)
    return finalList


def connectMysql_db():
    try:
        db = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Sanket@8081',
            database='covid19india'
        )
        return db
    except mysql.connector.Error as err:
        print("Failed to establish connection\n")


def insertRecords(db, vacc_details):
    try:
        db_cur = db.cursor()
        query = """INSERT INTO VACCINATION_DETAILS (CREATION_DATE,STATE,FIRST_DOSE,SECOND_DOSE,TOTAL_DOSES) VALUES (%s,
        %s,%s,%s,%s) """
        my_data = []
        for row in vacc_details:
            my_data.append(tuple(row))
        db_cur.executemany(query, my_data)
        db.commit()
    except mysql.connector.Error as err:
        db.rollback()
        print("Failed to insert into VACCINATION_DETAILS table {}".format(err))
    finally:
        db_cur.close()
        db.close()


# fileDownload()
db_object = connectMysql_db()
final_data = extractTablesFromPDF()
insertRecords(db_object, final_data)
