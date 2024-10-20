import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from functools import wraps
import os
import warnings
warnings.filterwarnings("ignore")



# first install mysql dump from https://chembl.gitbook.io/chembl-interface-documentation/downloads
# then
# engine_name = "mysql+pymysql://USERNAME:PASSWORD@/DB_NAME"
PATH = '/home/mrammar/uncert_da/'

def engine_dec(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        db_connection = create_engine("mysql+pymysql://mrammar:9561@/chembl_32")
        ans = fn(db_connection, *args, **kwargs)
        db_connection.dispose()
        return ans

    return wrapper


@engine_dec
def get_activity(db_connection: Engine) -> pd.DataFrame:

    activity = pd.read_sql(
        """
    SELECT DISTINCT a.activity_id as activity_chembl_id, molecule_dictionary.chembl_id as salt_chembl_id, 
    c.chembl_id as molecule_chembl_id, target_dictionary.chembl_id as target_chembl_id, 
    assays.chembl_id as assay_chembl_id, docs.chembl_id as document_chembl_id,
    a.standard_type, a.standard_value, a.bao_endpoint

    FROM activities a

    JOIN (
        SELECT molecule_dictionary.chembl_id, molecule_hierarchy.parent_molregno, molecule_hierarchy.molregno
        FROM molecule_hierarchy
        JOIN molecule_dictionary ON molecule_dictionary.molregno = molecule_hierarchy.parent_molregno
    ) c ON a.molregno = c.molregno

    JOIN molecule_dictionary ON a.molregno = molecule_dictionary.molregno
    JOIN assays ON a.assay_id = assays.assay_id
    JOIN docs ON a.doc_id = docs.doc_id
    JOIN target_dictionary ON assays.tid = target_dictionary.tid
    LEFT JOIN variant_sequences ON assays.variant_id = variant_sequences.variant_id

    WHERE (a.standard_type = 'IC50' OR a.standard_type = 'Ki')
    AND a.standard_units = 'nM'
    AND a.relation = '='
    """,
        con=db_connection,
    )

    return activity


@engine_dec
def get_document(db_connection: Engine) -> pd.DataFrame:
    document = pd.read_sql(
        """
    SELECT DISTINCT
    chembl_id as 'document_chembl_id', doi, title, abstract, journal, volume, issue, 
    first_page, last_page, year, pubmed_id, authors
    FROM
    docs
    WHERE
    doc_type = 'PUBLICATION' 
    AND chembl_id IS NOT NULL
    AND doi IS NOT NULL
    AND pubmed_id IS NOT NULL
    """,
        con=db_connection,
    )

    return document


@engine_dec
def get_molecule(db_connection: Engine) -> pd.DataFrame:
    molecule = pd.read_sql(
        """
    SELECT DISTINCT
    molecule_dictionary.chembl_id as salt_chembl_id, c.chembl_id as molecule_chembl_id, molecule_dictionary.molecule_type, molecule_dictionary.chirality, 
    compound_structures.canonical_smiles, compound_structures.standard_inchi_key, compound_properties.mw_freebase, 
    CONCAT_WS(', ', molecule_dictionary.pref_name, GROUP_CONCAT(DISTINCT molecule_synonyms.synonyms SEPARATOR ', ')) AS all_names

    FROM
    molecule_dictionary

    JOIN (
        SELECT molecule_dictionary.chembl_id, molecule_hierarchy.parent_molregno, molecule_hierarchy.molregno
        FROM molecule_hierarchy
        JOIN molecule_dictionary ON molecule_dictionary.molregno = molecule_hierarchy.parent_molregno
    ) c ON molecule_dictionary.molregno = c.molregno
    JOIN compound_structures on molecule_dictionary.molregno = compound_structures.molregno
    JOIN compound_properties ON molecule_dictionary.molregno = compound_properties.molregno
    LEFT JOIN molecule_synonyms ON molecule_dictionary.molregno = molecule_synonyms.molregno

    WHERE molecule_dictionary.molecule_type = 'Small molecule' 
    AND molecule_dictionary.structure_type = 'MOL'
    AND molecule_dictionary.chembl_id IS NOT NULL
    AND compound_structures.canonical_smiles IS NOT NULL
    GROUP BY molecule_dictionary.molregno
    """,
        con=db_connection,
    )

    return molecule


@engine_dec
def get_target(db_connection: Engine) -> pd.DataFrame:
    target = pd.read_sql(
        """
    SELECT DISTINCT
    chembl_id as target_chembl_id, organism
    FROM
    target_dictionary
    WHERE
    chembl_id IS NOT NULL 
    AND target_type = 'SINGLE PROTEIN'
    AND organism IS NOT NULL
    """,
        con=db_connection,
    )

    return target


@engine_dec
def get_assay(db_connection: Engine) -> pd.DataFrame:
    wr_assay = pd.read_sql(
        """
    SELECT DISTINCT assays.chembl_id AS assay_chembl_id
    FROM activities
    JOIN assays ON activities.assay_id = assays.assay_id
    WHERE (activities.standard_value IS NULL 
    OR activities.standard_value <= 0 
    OR activities.data_validity_comment = 'Outside typical range' 
    OR activities.data_validity_comment = 'Potential author error' 
    OR activities.data_validity_comment = 'Potential transcription error')
    AND (activities.standard_type = 'Ki' OR activities.standard_type = 'IC50')

    UNION

    SELECT DISTINCT assays.chembl_id AS assay_chembl_id
    FROM activities
    JOIN assays ON activities.assay_id = assays.assay_id
    WHERE activities.standard_type = 'IC50'
    GROUP BY activities.assay_id, activities.molregno
    HAVING COUNT(activities.molregno) > 1

    UNION

    SELECT DISTINCT assays.chembl_id AS assay_chembl_id
    FROM activities
    JOIN assays ON activities.assay_id = assays.assay_id
    WHERE activities.standard_type = 'Ki'
    GROUP BY activities.assay_id, activities.molregno
    HAVING COUNT(activities.molregno) > 1
    
    """,
        con=db_connection,
    )

    assay = pd.read_sql(
        """
    SELECT DISTINCT
    assays.chembl_id as assay_chembl_id, docs.chembl_id as document_chembl_id, 
    target_dictionary.chembl_id as target_chembl_id, assays.description, assays.bao_format,
    assays.assay_cell_type, assays.assay_subcellular_fraction, assays.assay_tissue           
    FROM
    assays
    JOIN docs on assays.doc_id = docs.doc_id
    JOIN target_dictionary on assays.tid = target_dictionary.tid
    WHERE
    (assays.assay_type = 'B' OR assays.assay_type = 'F')
    AND assays.confidence_score = 9 
    AND assays.relationship_type = 'D' 
    AND (assays.bao_format = 'BAO_0000357' OR assays.bao_format = 'BAO_0000219' OR assays.bao_format = 'BAO_0000366')
    AND assays.assay_strain IS NULL
    AND assays.chembl_id IS NOT NULL
    AND docs.chembl_id IS NOT NULL
    AND target_dictionary.chembl_id IS NOT NULL
    AND assays.description IS NOT NULL
    """,
        con=db_connection,
    )

    assay = assay[
        ~(assay.assay_chembl_id.isin(wr_assay.assay_chembl_id))
    ].reset_index(drop=True)
    assay["bao_format"] = assay["bao_format"].astype("category")
    return assay

@engine_dec
def get_protein_hierarchy(db_connection: Engine) -> pd.DataFrame:

    prot = pd.read_sql(
        """
    SELECT DISTINCT protein_classification.protein_class_id, protein_classification.parent_id, protein_classification.pref_name, protein_classification.short_name, protein_classification.class_level, target_dictionary.chembl_id as target_chembl_id

    FROM
    target_dictionary
    JOIN target_components on target_dictionary.tid = target_components.tid
    join component_class on target_components.component_id = component_class.component_id
    join protein_classification on component_class.protein_class_id = protein_classification.protein_class_id
    """,
        con=db_connection,
    ).fillna("").sort_values("protein_class_id", ascending=False)

    return prot

@engine_dec
def get_protein_classification(db_connection: Engine) -> pd.DataFrame:
    prot_class = pd.read_sql("SELECT * from protein_classification", con=db_connection)
    return prot_class

def save_results() -> None:
    for _ in (get_activity, get_assay, get_document, get_molecule, 
              get_target, get_protein_classification, get_protein_hierarchy):
        name = _.__name__[4:]
        if (name + '.prqt') not in os.listdir(PATH + 'data/raw_data'):
            df = _()
            df.to_parquet(PATH + f"data/raw_data/{name}.prqt")

save_results()