import pandas as pd
from SPARQLWrapper import JSON, SPARQLWrapper


def get_uniprot_information():
    sparql = SPARQLWrapper("http://sparql.uniprot.org/sparql")
    sparql.setQuery(
        """
    PREFIX up:<http://purl.uniprot.org/core/>
    PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>

     SELECT DISTINCT 
        (STRAFTER(STR(?protein), "http://purl.uniprot.org/uniprot/") AS ?target_uniprot_id) 
        (STRAFTER(STR(?chemblEntry), "http://rdf.ebi.ac.uk/resource/chembl/target/") AS ?target_chembl_id) 
        (REPLACE(
        GROUP_CONCAT(DISTINCT CONCAT(
            IF(BOUND(?proteinName), CONCAT(?proteinName, ', '), ''),
            IF(BOUND(?shortName), CONCAT(?shortName, ', '), ''),
            IF(BOUND(?altName), CONCAT(?altName, ', '), ''),
            IF(BOUND(?saltName), CONCAT(?saltName, ', '), ''),
            IF(BOUND(?gene_name), CONCAT(?gene_name, ', '), ''),
            IF(BOUND(?alt_gene_name), CONCAT(?alt_gene_name, ', '), '')
        ); separator = ''), ', $', '') AS ?target_names) 
        (COUNT(DISTINCT ?isoform) AS ?isoforms) 
        ?pH_dependence

        WHERE { 
            ?protein a up:Protein ; 
                     rdfs:seeAlso ?chemblEntry .

            OPTIONAL { 
                ?protein up:recommendedName ?name .
                OPTIONAL { ?name up:fullName ?proteinName } . 
                OPTIONAL { ?name up:shortName ?shortName } . 
            }
            OPTIONAL {
                ?protein up:alternativeName ?alternativeName .
                OPTIONAL { ?alternativeName up:fullName ?altName } .
                OPTIONAL { ?alternativeName up:shortName ?saltName } .
            }
            OPTIONAL {
                ?protein up:annotation ?annotation .
                ?annotation a up:PH_Dependence_Annotation .
                ?annotation rdfs:comment ?pH_dependence .
            }
            OPTIONAL {
                ?protein a up:Protein .
                ?protein up:sequence ?isoform .
            }
            OPTIONAL {
                ?protein up:encodedBy ?gene .
                ?gene skos:prefLabel ?gene_name .
            }
            OPTIONAL {
                ?protein up:encodedBy ?gene .
                ?gene skos:altLabel ?alt_gene_name .
            } 	

            ?chemblEntry up:database <http://purl.uniprot.org/database/ChEMBL> . 
        } 

        GROUP BY ?protein ?chemblEntry ?pH_dependence
    """
    )
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    cols = results["head"]["vars"]
    data = []
    for row in results["results"]["bindings"]:
        item = []
        for _ in cols:
            item.append(row.get(_, {}).get("value"))
        data.append(item)

    return pd.DataFrame(data, columns=cols)


PATH = "/home/mrammar/uncert_da/"
uniprot_info = get_uniprot_information()
uniprot_info.to_parquet(PATH + "data/raw_data/uniprot_info.prqt")
#uniprot_info.to_csv("uniprot_info.csv", sep=";", index=False)