from dataclasses import dataclass

import pandas as pd

from models.PipelineStarter.Ressources.ManualConceptIDs import conceptsIDs
from models.Processing.Pipelines.BasePipeline import BasePipeline

observationMapping = {
    'praemed_asa': 'ASA Physical Status Classification',
    'praemed_rf_erhebung': 'Preoprative Assessment of Delirium Risk Factors',
    'praemed_rf_team': 'Discussion of risk factors and preventive strategies among care givers',
    'praemed_rf_opti': 'Optimization of risk factors for delirium',
    'praemed_rf_opti_moeglich': 'Presence of optimizable Risk factors for Delirium',
    'op_eeg_used': 'Intraoperative use of EEG',
    'op_eeg_bs_minuten': 'Presence of Intraoperative',
    'op_eeg_bs_ratio': 'Burst Suppression Ration',
    'medeinzel_dex_prev': 'Use of Dexmedetomedin as prevention measure against delirium',
    'praemed_rf_praedi': 'Presence of Predisposing Risk Factors for Delirium',
    'praemed_rf_praezi': 'Presence of Precipitating Risk Factors for Delirium',
    'mobil_selbst': 'Does the patient mobilize independently?',
    'nutri_selbst': 'Does the patient nourish/feed themselves independently?'
}


observationBooleanMapping = {
    'mobil_selbst': (conceptsIDs.get('able_walk'), conceptsIDs.get('unable_walk')),
    'nutri_selbst': (conceptsIDs.get('able_eat'), conceptsIDs.get('unable_eat'))
}


@dataclass
class ObservationPipeline(BasePipeline):
    '''
    Lembrar que alguns dados que estao ali em cima podem nao estar no xml!
    '''
    def process(self):
        if self.rawData:
            mobil = self.__processMobil(self.rawData.get('mobilization', pd.DataFrame()))
            nutri = self.__processNutri(self.rawData.get('nutrition', pd.DataFrame()))
            #praeOp = self.__processPraedi(self.rawData.get('predispositionfactors', pd.DataFrame()))

            processedDf = self._adaptSchema(mobil, nutri)

            if isinstance(processedDf, pd.DataFrame):
                processedDf = self.__createDateColumns(processedDf)
                processedDf['observation_type_concept_id'] = conceptsIDs.get('observation_type_concept_id')

            return processedDf
        
    def __processNutri(self, df):
        if 'nutri_selbst' in df.columns:
            df = df[['nutri_selbst', 'visit_datetime', 'casenumber']].dropna()

            if not df.empty:
                df = self._addPersonID(df)
                df = self.__addNewColums(df, 'nutri_selbst')
                df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'nutri_selbst', 'observation_source_value'], self.idCol)
                df = self._addOMOPConceptCols(df)
                df.rename(columns={'visit_datetime': 'observation_datetime', 'nutri_selbst': 'value_source_value'}, inplace=True)

            return df
        
    def __processMobil(self, df):
        if 'mobil_selbst' in df.columns:
            df = df[['mobil_selbst', 'visit_datetime', 'casenumber']].dropna()

            if not df.empty:
                df = self._addPersonID(df)
                df = self.__addNewColums(df, 'mobil_selbst')
                df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'mobil_selbst', 'observation_source_value'], self.idCol)
                df = self._addOMOPConceptCols(df)
                df.rename(columns={'visit_datetime': 'observation_datetime', 'mobil_selbst': 'value_source_value'}, inplace=True)

            return df


    def __processPraedi(self, df):
        if 'praemed_rf_praedi' in df.columns:
            df = df.loc[df['praemed_rf_praedi']].dropna()

            if not df.empty:
                df = self._addPersonID(df)
                df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'praemed_rf_praedi'], self.idCol)
                df['observation_source_value'] = 'Predisposing Risk Factors for Delirium'
                
                df.rename(columns={'visit_datetime': 'observation_datetime'}, inplace=True)

        return df

    def __processPraeopObs(self, df):
        '''
        TEREI AQUI QUE CONCATERNAR AS COLUNAS DOS DOIS DATAFRAMES -> 'predispositionfactors' terá as colunas 'praemed_rf_erhebung' & 'praemed_rf_praedi' / 'precipitatingfactors' terá as colunas 'praemed_rf_erhebung' & 'praemed_rf_praezi'
        isso antes de adicionar o person! Remover duplicatas
        ADICIONAR no problema de concatenao OU depois averiguacao se dataframe resultante está vazio ou nao
        '''
        df = self._addPersonID(df)

        dfsCols = [
            (['person_id', 'praemed_datetime', 'praemed_rf_praedi'], False),
            (['person_id', 'praemed_datetime', 'praemed_rf_praezi'], False),
            (['person_id', 'praemed_datetime', 'praemed_asa'], False),
            (['person_id', 'praemed_datetime', 'praemed_rf_team'], False),
            (['person_id', 'praemed_datetime', 'praemed_rf_opti'], False),
            (['person_id', 'praemed_datetime', 'praemed_rf_opti_moeglich'], False)
        ]

        return self.__processDfs(df, dfsCols)

    @staticmethod
    def __extractColumnsAndDropMissings(df, cols):
        return df[cols].replace('', pd.NA).dropna()

    @staticmethod
    def __addNewColums(newDf, col, asNumber=False):
        obs_val = observationMapping.get(col, 'Unknown Observation')
        newDf['observation_source_value'] = obs_val

        if asNumber:
            newDf['value_as_number'] = pd.to_numeric(newDf[col], errors='coerce')
        else:
            id_for_zero, id_for_one = observationBooleanMapping.get(col, (conceptsIDs.get('absent'), conceptsIDs.get('present')))
            newDf['value_as_concept_id'] = newDf[col].map({False: id_for_zero, True: id_for_one}).fillna(pd.NA)

        return newDf

    @staticmethod
    def __createDateColumns(df):
        df = df.dropna(subset=['observation_datetime'])
        df['observation_date'] = df['observation_datetime'].dt.date
        return df
