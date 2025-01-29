import hashlib

from dataclasses import dataclass
from pandas import DataFrame


@dataclass
class UniqueIDCreator:
    @classmethod
    def createUniqueID(cls, df: DataFrame, cols: list[str], idColName: str = 'unique_id', duplicatesAllowed=False) -> DataFrame:
        df_selected = df[cols].copy()

        df_selected['combined'] = df_selected.apply(lambda row: '_'.join(map(str, row.fillna('NA').values)), axis=1)

        if not duplicatesAllowed:
            df_selected = cls.__handleDuplicatedRows(df_selected)

        df_selected[idColName] = df_selected['combined'].apply(lambda x: cls.__hash_to_int(x.lower()))

        if df_selected[idColName].duplicated().any() and not duplicatesAllowed:
            raise ValueError("Generated IDs are not unique!")

        df[idColName] = df_selected[idColName]

        return df

    @staticmethod
    def __handleDuplicatedRows(df_selected):
        duplicates = df_selected.duplicated(subset='combined', keep=False)
        df_selected.loc[duplicates, 'combined'] = df_selected['combined'] + df_selected.loc[duplicates].groupby('combined').cumcount().astype(str).radd('_')
        return df_selected

    @staticmethod
    def __hash_to_int(x, digest_size=3):
        blake2b_hash = hashlib.blake2b(digest_size=digest_size)
        blake2b_hash.update(x.encode('utf-8'))
        hash_int = int(blake2b_hash.hexdigest(), 16)
        return hash_int
