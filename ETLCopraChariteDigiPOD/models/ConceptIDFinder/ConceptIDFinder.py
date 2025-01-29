import pandas as pd
from dataclasses import dataclass, field


@dataclass
class OMOPConceptIDMapper:
    conceptID: pd.DataFrame
    conceptRelationship: pd.DataFrame
    sourceToConcept: pd.DataFrame
    localToLocal: pd.DataFrame

    def __post_init__(self):
        self.__setColumnTypes(self.conceptID, ['concept_id', 'concept_code'])
        self.__setColumnTypes(self.conceptRelationship, ["concept_id_1", "concept_id_2"])

    def __setColumnTypes(self, df, cols):
        for column in cols:
            df[column] = df[column].astype(str)

    def mapLocalCodeToLocal(self, df, conceptCol):
        df = df.merge(self.localToLocal, left_on=conceptCol, right_on='Variabelname', how='inner')
        return df
    
    def mapSourceCodeToConcepts(self, df, idCol):
        foundConcepts = df.merge(self.sourceToConcept, on='source_code', how='inner') \
                          .loc[:, [idCol, 'target_concept_id']]
        foundConcepts.rename(columns={'target_concept_id': 'concept_id_1'}, inplace=True)
        return foundConcepts

    def mapSourceConceptToConcepts(self, df, idCol, conceptCol):
        foundConcepts = df.merge(self.conceptID, left_on=conceptCol, right_on='concept_code', how='inner') \
                          .loc[:, [idCol, 'concept_id']]
        foundConcepts.rename(columns={'concept_id': 'concept_id_1'}, inplace=True)
        return foundConcepts

    def mapConceptsToStandardConcepts(self, df, idCol):
        filteredConceptRel = self.conceptRelationship.loc[self.conceptRelationship['concept_id_1'].isin(df['concept_id_1'].unique())]
        self.cr_dict = dict(zip(filteredConceptRel['concept_id_1'], filteredConceptRel['concept_id_2']))
        df['concept_id'] = df['concept_id_1'].map(self.cr_dict)
        df = df.loc[df['concept_id'].notna(), [idCol, 'concept_id']]

        filteredConceptID = self.conceptID.loc[self.conceptID['concept_id'].isin(df['concept_id'].unique())]
        self.domain_map = dict(zip(filteredConceptID['concept_id'], filteredConceptID['domain_id']))
        df['domain_id'] = df['concept_id'].map(self.domain_map)
        df.rename(columns={'concept_id': 'standard_concept_id'}, inplace=True)

        return df


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
