import pandas as pd
import numpy as np
from itertools import combinations
from tqdm import tqdm
import os
import warnings

warnings.filterwarnings('ignore')

PATH = '/home/mrammar/uncert_da/data/raw_data/'
def comb(group):
    return pd.DataFrame(list(combinations(group.values, 2)), columns=["1", "2"])

def generate_pairs():
    activity = pd.read_parquet(PATH + "activity.prqt")
    groups = activity.groupby(
        ["standard_type", "molecule_chembl_id", "target_chembl_id"]
    )
    cols = activity.columns
    pair_list = []

    for index, group in tqdm(groups, desc='Processing activity groups'):
        if len(group) > 1:

            group = (
                group.groupby(
                    ["standard_type", "molecule_chembl_id", "target_chembl_id"]
                )
                .apply(comb)
                .reset_index(level=1, drop=True)
            )
            group = pd.concat(
                [
                    pd.DataFrame(
                        group["1"].to_list(), columns=[i + "1" for i in cols]
                    ),
                    pd.DataFrame(
                        group["2"].to_list(), columns=[i + "2" for i in cols]
                    ),
                ],
                axis=1,
            )
            pair_list.append(group)

    all_pairs = pd.concat(pair_list, ignore_index=True)
    all_pairs["delta_log_activity"] = abs(
        np.log10(all_pairs.standard_value1)
        - np.log10(all_pairs.standard_value2)
    )
    if 'all_pairs.prqt' not in os.listdir(PATH):
        all_pairs.to_parquet(PATH + "pairs.prqt")

generate_pairs()


#===================================================================
def generate_fours(all_pairs):

    fours_df = (
        all_pairs.query("step5 == True")
        .groupby(["standard_type", "assay_chembl_id1", "assay_chembl_id2"])
        .filter(lambda x: len(x) > 1)
    )
    fours_df = fours_df[
        [
            "standard_type",
            "assay_chembl_id1",
            "assay_chembl_id2",
            "molecule_chembl_id",
            "target_chembl_id",
            "activity_chembl_id1",
            "activity_chembl_id2",
            "standard_value1",
            "standard_value2",
        ]
    ]
    groups = fours_df.groupby(
        ["standard_type", "assay_chembl_id1", "assay_chembl_id2"]
    )

    cols = fours_df.columns
    four_list = []

    for index, group in tqdm(groups):
        if len(group) > 1:

            group = (
                group.groupby(
                    ["standard_type", "assay_chembl_id1", "assay_chembl_id2"]
                )
                .apply(comb)
                .reset_index(level=1, drop=True)
            )
            group = pd.concat(
                [
                    pd.DataFrame(
                        group["1"].to_list(),
                        columns=[i + "_pair1" for i in cols],
                    ),
                    pd.DataFrame(
                        group["2"].to_list(),
                        columns=[i + "_pair2" for i in cols],
                    ),
                ],
                axis=1,
            )
            four_list.append(group)

    all_fours = pd.concat(four_list, ignore_index=True)

    final_fours = all_fours[
        [
            "standard_type_pair1",
            "assay_chembl_id1_pair1",
            "assay_chembl_id2_pair1",
            "molecule_chembl_id_pair1",
            "molecule_chembl_id_pair2",
            "target_chembl_id_pair1",
            "activity_chembl_id1_pair1",
            "activity_chembl_id2_pair1",
            "activity_chembl_id1_pair2",
            "activity_chembl_id2_pair2",
            "standard_value1_pair1",
            "standard_value2_pair1",
            "standard_value1_pair2",
            "standard_value2_pair2",
        ]
    ]
    final_fours = final_fours.rename(
        columns={
            "standard_type_pair1": "standard_type",
            "assay_chembl_id1_pair1": "assay_chembl_id1",
            "assay_chembl_id2_pair1": "assay_chembl_id2",
            "target_chembl_id_pair1": "target_chembl_id",
        }
    )
    final_fours.to_parquet(PATH + "all_fours.prqt")
    return final_fours
