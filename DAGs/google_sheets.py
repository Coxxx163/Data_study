#импорт библиотек
import gspread
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#подставь свои переменные подключения к базе данных
login = 'sergey_r'
password = 'nfgete237'
host = '94.26.239.237'
db = 'dwh'

url = 'https://docs.google.com/spreadsheets/d/1ObmR1oDZWN3peAkVpUX5XZAdNO_HmgAoLhJdbz3kf3Q'

engine = create_engine(
    f'postgresql+psycopg2://{login}:{password}@{host}/{db}')

#поменяй значения переменной service_acc, ниже приведен пример структуры, но аккаунт не рабочий
service_acc = {
  "type": "service_account",
  "project_id": "idyllic-web-481811-p7",
  "private_key_id": "f69a933beb476286f18ed51b4d823f123b8ef3fc",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDeBD85NwTcSaDz\njRozIQLqZlMX7CPSXZIlk4nySoNZxb/ocbcfLNxgPNxra3ra02wdGJdEn58DXoeJ\n4j+3L+07XQ/3RN9cE4ScM59IgxzjKJuuc1vvyhtx6VedHGDhC2ILKFekykTsfcqd\na9IaQBT4lH7ef+nHnjcKRD4W5pCKKYXMqjXabfcoPMItT5N7xziQH/tgKmREWqjg\n1fBrnBU00UUvwcjYS2PyAQPo8z6nT1VdKHFcZpCSQjaAavkWWVQnXvkkZJOMjd9N\nrgY6p2Rn3+AvTOzB222JdRRKPN8h5tZ6CyE4ix2ghXKntAUvd5XZy5anJtCDSfDW\nbGjZB/d9AgMBAAECggEALa0zgbaxVA385GaxD3hY9HchXNBbLVZNcVgpnxTX8cIG\nwdCVc39EBrO2yI3oDHnHajQLORXa/fM2kPZgk/RzCw3SMVX02ylz1LwlJuK8sHqE\nDw/SSCLtYxH2Ec+LGfnAzdlasL9hj0MxCpKqevaYqezJLkckRoqlOcJBAO6mKZpP\nwRQdwXCGn9+s76fFBMndFSXg8La5pBqEOfnMnCDhVceVlAt0toifcR6IRqwrUuTK\nFRxB/L1l56LrJoNggHjPaZd2FaMEKM9fn3DaWcOK4DY/LMgtZxtGoAI98DDlrklt\nXl+27xAxt5DHaqmj9T324KMMjuqEuVGLMmGV/UFh+QKBgQDuoNZCvmddA/MHNYN4\ndDXIZYOb9qkJoi6O1Aj47SfKAWVITt/6T6fD/Q+aEiEwEapWmfIPAm6YKAcBeqnD\nNO0LVgizazPlPf/96Iq9Fg+3THnhYaEZOghUxSW8Si8MsyWk7IMbCPiBaTnHCH+d\nRdqoLCDqkV0eXiUMnzFC/rTqWQKBgQDuLdQm83v2GaGA/u6sB0y/U2Y8V58gri2i\nJnhN44ULmQUtsfEl0dl8PRLD5w4XqK5cWCGpzXEsPiXezR4PG7NwSa5DFRjlB4BU\ngR6jUmshlGqKPWepQty2uXsMBuRrabxEsyWVLDu/DlshaR7amlTeGRgEQSH08Vk6\nPr6NINaJxQKBgDcuZD2s8BQXXLS3ED7jiKhdqTCAupgqs999ctG1bbUN25D8BFlK\n4D23IEt9k8Uc4KoEEXCdHFv9LZmO9wDYFVTAwWRUDv0c+DyL8MFG/71gZvLSc8F0\npQaNTGudMAIcz0CzHKI80dKDNcQr1+7Tf3vSMI5trP3fwWuIyIh4g/eJAoGBAI0+\nACFZIpm+eVfV1hOOAKU0/9uo2mS25bjSt2T5F1zZUYjXjPmwiBh03zvtOR4umbeJ\nS+02N3bVJAAMA1dSRgbAquZ9DFZFLITwTQrtRTqUmNmBsIdfbyOsuptXOv23pj3X\nPuJHaObTzJbzj6hy/QD4QJXFSi3HMfKoYreuUYtBAoGBAMZSp9A+rao2kfQ94bsU\nIDFfmrm/ZVHkaD/Mpo2L435hUVhjo157+UUbnatpy8LoU6X2BTyeXA5vQoPhOIzT\nRQ9vdd3MKq80uVvYHO0wYrlpxpRZ8uhRwBJuzfWmfkFRxfqSGbp5cKOzft5KSIxb\n3Rzp59QbpgRl2SZER22yDp88\n-----END PRIVATE KEY-----\n",
  "client_email": "google-sheets@idyllic-web-481811-p7.iam.gserviceaccount.com",
  "client_id": "113463874255938446006",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/google-sheets%40idyllic-web-481811-p7.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}


Client = gspread.service_account_from_dict(service_acc)
Spreadsheet = Client.open_by_url(url)

#Переименование колонок
COLUMN_MAPPING = {
    'тип коммуникации': 'communication_type',
    'провайдер': 'provider',
    'тип отправки': 'send_type',
    'тариф': 'tariff',
    'прайс': 'price'
}

#порядок листов
SHEET_ORDER = ['2025Q1', '2025Q2', '2025Q3', '2025Q4', '2026Q1']

#перевод имени листа в дату 
def sheet_name_to_date(sheet_name):
    year = int(sheet_name[:4])
    quarter = int(sheet_name[-1])
    month = (quarter - 1) * 3 + 1
    return pd.Timestamp(year=year, month=month, day=1)

#функция для считывания данных с указанного листа sheet_name, переименования колонок, добавления sheet_date, timestamp
def get_data(sheet_name):
    ws = Spreadsheet.worksheet(sheet_name)
    df = pd.DataFrame(ws.get_all_records(value_render_option="UNFORMATTED_VALUE"))
    if df.empty:
        return pd.DataFrame()

    df = df.rename(columns=COLUMN_MAPPING)
    df['sheet_date'] = sheet_name_to_date(sheet_name)
    df['load_ts'] = datetime.now()
    return df
    
# Основная функция для загрузки всех листов в одну таблицу
def load_sheets_to_postgres():
    table_name = 'google_sheets_data'
    schema_name = 'sergey_r'

    dfs = [get_data(sheet) for sheet in SHEET_ORDER]
    dfs = [df for df in dfs if not df.empty]

    if not dfs:
        return

    final_df = pd.concat(dfs, ignore_index=True)

    final_df.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists='replace',   # <-- ключевой момент
        index=False,
        method='multi',
        chunksize=1000
    )

# DAG

OWNER = "{{ OWNER }}"  # обеспечивает уникальность дагов по ученикам

with DAG(
    dag_id=f'google_sheets_to_DB_{OWNER}',        # название DAG с учетом OWNER
    start_date=datetime(2025, 11, 30),
    schedule_interval='0 12 1 * *',    # расписание запуска DAG
    catchup=False,
    tags=[OWNER],
    default_args={"owner": OWNER}
) as dag:
    google_sheets_to_DB = PythonOperator(
    task_id='load_google_sheets',
    python_callable=load_sheets_to_postgres,
    op_kwargs={
        'table_name': 'google_sheets_data'
    }
)
google_sheets_to_DB
