# <YOUR_IMPORTS>
import os

import dill
import pandas as pd
import json

from pydantic import BaseModel

path = os.environ.get('PROJECT_PATH', '/opt/airflow/plugins')

files = os.listdir(f'{path}/data/models')
pkl_files = [f for f in files if f.endswith('.pkl')]
pkl_files.sort()
latest_model_file = pkl_files[-1]
with open(f'{path}/data/models/{latest_model_file}', 'rb') as file:
    model = dill.load(file)

files = os.listdir(f'{path}/data/test')
class Form(BaseModel):
    id: int
    url: str
    region: str
    region_url: str
    price: int
    year: float
    manufacturer: str
    model: str
    fuel: str
    odometer: float
    title_status: str
    transmission: str
    image_url: str
    description: str
    state: str
    lat: float
    long: float
    posting_date: str

final_df = pd.DataFrame()

def save_to_csv():
    final_df.to_csv(f'{path}/data/predictions/predictions.csv', encoding='utf-8')

def predict():
    global final_df
    for file in files:
        path_str = f'{path}/data/test/{file}'
        with open(path_str, 'r') as f:
            data = json.load(f)
        form = Form(**data)
        df = pd.DataFrame([form.dict()])
        y = model.predict(df)
        row = pd.DataFrame({'file': [file.split('.')[0]], 'prediction': [y[0]]})
        final_df = pd.concat([final_df, row], ignore_index=True)
    save_to_csv()


if __name__ == '__main__':
    predict()
