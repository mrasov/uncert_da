import requests
import json
import pandas as pd
from tqdm import tqdm


data_from_pubmed = pd.read_excel('../data/raw_data/data_from_pubmed.xlsx', sheet_name='core journals')
data_from_pubmed = data_from_pubmed.drop_duplicates(subset = 'PMID').reset_index(drop = True)

doi_list = data_from_pubmed.DOI.values
refs = []

for el in tqdm(doi_list):
    response = requests.get(f'https://api.crossref.org/works/{el}')
    if response.status_code == 200:
        response = response.json()
        refs.append(response['message']['reference-count'])
    else:
        refs.append(None)

refs.to_csv('pubmed_reference.csv', sep=',')