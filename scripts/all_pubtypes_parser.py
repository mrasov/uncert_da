import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
import requests
from tqdm import tqdm

chembl_32 = pd.read_excel('ChEMBL_32_documents.xlsx', sheet_name='Sheet1')

pubmed_id_list = chembl_32.pubmed_id.values
all_pubtypes = []
for pmid in tqdm(pubmed_id_list):
    html_code = bs(requests.get(f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/").text, 'lxml').find('div',
                                                                                               class_='publication-types keywords-section')
    soup = bs(f"""{html_code}""", 'html.parser')
    publication_types = []
    for button in soup.select('.keyword-actions-trigger'):
        publication_types.append(button.get_text(strip=True))

    all_pubtypes.append(publication_types)

pd.DataFrame(all_pubtypes).to_csv('all_pubtypes.csv', index=False)
