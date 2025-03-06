import pandas as pd

from dataclasses import dataclass, field
from sqlalchemy.engine import Engine


@dataclass
class OMOPConceptIDMapper:
    engine: Engine
    
    sourceToConcept: pd.DataFrame
    localToLocal: pd.DataFrame

    concept_table: str = "concept"
    concept_relationship_table: str = "concept_relationship"
    relationship_id: str = "Maps to"
    schema: str = "cds_cdm"


    def mapLocalCodeToLocal(self, df, conceptCol):
        df = df.merge(self.localToLocal, left_on=conceptCol, right_on='Variabelname', how='inner')
        return df
    
    def mapSourceCodeToConcepts(self, df, idCol):
        foundConcepts = df.merge(self.sourceToConcept, on='source_code', how='inner') \
                          .loc[:, [idCol, 'target_concept_id']]
        foundConcepts.rename(columns={'target_concept_id': 'concept_id_1'}, inplace=True)
        return foundConcepts

    def mapSourceConceptToConcepts(self, df, idCol, conceptCol):
        unique_codes = df[conceptCol].dropna().unique().tolist()
        
        placeholder_codes = ",".join(["%s"] * len(unique_codes))
        query = f"""
            SELECT CAST(concept_id AS TEXT), concept_code
            FROM {self.schema}.{self.concept_table}
            WHERE concept_code IN ({placeholder_codes})
        """
        concept_map = pd.read_sql(query, self.engine, params=unique_codes)

        merged = df.merge(
            concept_map, 
            left_on=conceptCol, 
            right_on='concept_code', 
            how='inner'
        )[[idCol, 'concept_id']]

        merged.rename(columns={'concept_id': 'concept_id_1'}, inplace=True)
        return merged

    def mapConceptsToStandardConcepts(self, df, idCol):
        concept_ids = (
            df['concept_id_1']
            .dropna()
            .unique()
            .tolist()
        )

        if not concept_ids:
            empty_df = pd.DataFrame(columns=[idCol, 'standard_concept_id', 'domain_id'])
            empty_df[idCol] = empty_df[idCol].astype(df[idCol].dtype)
            return empty_df

        placeholder_ids = ",".join(["%s"] * len(concept_ids))
        domain_query = f"""
            SELECT CAST(concept_id AS TEXT), domain_id
            FROM {self.schema}.{self.concept_table}
            WHERE concept_id IN ({placeholder_ids})
        """

        domain_params = tuple(concept_ids)
        domain_map = pd.read_sql(domain_query, self.engine, params=domain_params)

        df['concept_id_1'] = df['concept_id_1'].astype(str)
        df.rename(columns={'concept_id_1': 'standard_concept_id'}, inplace=True)
        

        final_df = df.merge(
            domain_map, 
            left_on='standard_concept_id', 
            right_on='concept_id', 
            how='left'
        )[[idCol, 'standard_concept_id', 'domain_id']]

        return final_df


@dataclass
class DomainIDMapper:
    domainIDmap: pd.DataFrame

    def addConceptIDCols(self, df, tableCDM, idCol):
        domainIDvals = df['domain_id'].unique()
        for domainID in domainIDvals:
            df = self._addSingleColumn(df, domainID, tableCDM, idCol)
        return df

    def _addSingleColumn(self, df, domainID, tableCDM, idCol):
        try:
            tableField = self.domainIDmap.query(f'CDM_Table == "{tableCDM}" and domain_id == "{domainID}"')['Field'].values[0]
            tempDf = df[df['domain_id'] == domainID].loc[:, [idCol, 'standard_concept_id']].rename(columns={'standard_concept_id': tableField})
            df = df.merge(tempDf, on=idCol, how='outer')
            return df
        except IndexError:
            return df
        except KeyError:
            return df


@dataclass
class OMOPMapper:
    conceptIDMapper: OMOPConceptIDMapper
    domainIDMapper: DomainIDMapper

    def __getattr__(self, name):
        if hasattr(self.conceptIDMapper, name):
            return getattr(self.conceptIDMapper, name)
        elif hasattr(self.domainIDMapper, name):
            return getattr(self.domainIDMapper, name)
        else:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
