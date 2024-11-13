import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
from tqdm import tqdm


data_from_pubmed = pd.read_excel('data_from_pubmed.xlsx', sheet_name='core journals').drop_duplicates(subset = 'PMID').reset_index(drop = True)
pubmed_id = data_from_pubmed['PMID'].values
abstracts = []

for i in tqdm(pubmed_id):
    f = requests.get(f"https://pubmed.ncbi.nlm.nih.gov/{int(i)}/")
    if str(f) == '<Response [200]>':
        ff = bs(f.text, 'lxml').find('div', class_='abstract-content selected')
        if ff == None:
            abstracts.append(None)
        else:
            abstr = str(ff.text)
            abstracts.append(abstr.split())
    else:
        abstracts.append(None)

abstracts_new = []
for el in abstracts:
    abst = ''
    if el != None:
        for k in el:
            abst += ' ' + k
        abstracts_new.append(abst)
    else:
        abstracts_new.append(None)

abstracts_new.to_csv('abstract_pubmed.csv', index=False)