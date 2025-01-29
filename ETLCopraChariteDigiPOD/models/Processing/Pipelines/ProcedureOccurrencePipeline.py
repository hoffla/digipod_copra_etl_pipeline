import re
from dataclasses import dataclass

import pandas as pd

from models.PipelineStarter.Ressources.ManualConceptIDs import conceptsIDs
from models.Processing.Pipelines.BasePipeline import BasePipeline

procedureMapping = {
    'angst_bewaltigung_typ___1': 'Non-pharmacological Treatment of Fear/Anxiety',
    'kog_erfolgt': 'Cognitive Stimulation',
    'orientierung_erfolgt': 'Orientation Measures',
    'kommuni_erfolgt': 'Required Communication Aid Provided',
    'circrhy_wie___1': 'Non-pharmacological Support of Circadian Rhythmus',
    'mobil_erfolgt': 'Patient Mobilisation',
    'nutri_erfolgt': 'Patient Enteral Feeding',
    'schluck_behandlung': 'Dysphagia Treatment',
    'schluck_nutri_umstellung': 'Dysphagia related change of Nutrition',
    'mundhyg_erfolgt': 'Mouth-Hygiene Measures',
}


@dataclass
class ProcedureOccurrencePipeline(BasePipeline):
    def process(self):
        if self.rawData:
            posOpProcedures = self.__processPosOpProcedures(self.rawData.get('postoperative_visi_arm_2'))
            operation = self.__processOp(self.rawData.get('operation_arm_2'))

            df = pd.concat([posOpProcedures, operation])

            df["procedure_type_concept_id"] = conceptsIDs.get("procedure_type_concept_id")
            processedDf = self._adaptSchema(df)

            return processedDf

    def __processPosOpProcedures(self, df):
        df = self._addPersonID(df)

        dfsCols = [
            ['person_id', 'angst_bewaltigung_typ___1', 'angst_bewaltigung_nichtmed_datetime'], # Quero só o com value == '1'!!
            ['person_id', 'kog_erfolgt', 'kog_datetime'],
            ['person_id', 'orientierung_erfolgt', 'orientierung_datetime'],
            ['person_id', 'kommuni_erfolgt', 'kommuni_datetime'],
            ['person_id', 'circrhy_wie___1', 'circrhy_datetime'], # Quero só o com value == '1'!!
            ['person_id', 'mobil_erfolgt', 'mobil_datetime'],
            ['person_id', 'nutri_erfolgt', 'nutri_datetime'],
            ['person_id', 'schluck_behandlung', 'schluck_behandlung_datetime'],
            ['person_id', 'schluck_nutri_umstellung', 'schluck_nutri_umstellung_datetime'],
            ['person_id', 'mundhyg_erfolgt', 'mundhyg_datetime'],
        ]

        return self.__processDfs(df, dfsCols)

    def __processDfs(self, df, dfsCols):
        posOpsProcedures = pd.DataFrame(columns=[self.idCol, 'person_id'])
        for dfCols in dfsCols:
            newDf = self.__createDataframe(df, dfCols)
            if isinstance(newDf, pd.DataFrame):
                posOpsProcedures = pd.concat([posOpsProcedures, newDf])

        posOpsProcedures['procedure_date'] = posOpsProcedures['procedure_datetime'].dt.date
        return posOpsProcedures

    def __createDataframe(self, df, cols) -> pd.DataFrame:
        newDf = self.__extractColumnsAndDropMissings(df, cols)
        if not newDf.empty:
            newDf = self.__addNewColums(newDf, cols)
            newDf = self.__createUniqueID(newDf, ['person_id', cols[2], 'procedure_source_value'], self.idCol)
            newDf = self._addOMOPConceptCols(newDf)
            newDf['procedure_datetime'] = pd.to_datetime(newDf[cols[2]], format='mixed')

            return newDf

    @staticmethod
    def __extractColumnsAndDropMissings(df, cols):
        return df[cols].replace('', pd.NA).dropna()

    @staticmethod
    def __addNewColums(newDf, cols):
        newDf['procedure_source_value'] = procedureMapping.get(cols[1], 'Unknown Procedure')
        return newDf

    def __processOp(self, df):
        df = self._addPersonID(df)
        df = self.__separateOPSCodes(df)
        df = self.__defineDateCols(df)
        df = self._createUniqueID(df, ['person_id', 'procedure_date', 'redcap_repeat_instance', 'op_ops'], self.idCol)
        df = self._addNonStandardOMOPConceptCols(df, 'op_ops')
        df = self.__addCustomMappingOPS(df)
        df.rename(columns={'op_ops': 'procedure_source_value', 'concept_id_1': 'procedure_source_concept_id'}, inplace=True)

        return df

    @staticmethod
    def __separateOPSCodes(df):
        opsExtra = df.drop(columns=['op_ops'])
        opsExtra['op_ops_extra'] = opsExtra['op_ops_extra'].str.split(',')
        df_exploded = opsExtra.explode('op_ops_extra')
        df_exploded['op_ops'] = df_exploded['op_ops_extra'].str.strip()

        dfSeparetedOps = pd.concat([df, df_exploded])
        dfSeparetedOps.drop(columns=['op_ops_extra'], inplace=True)
        dfSeparetedOps = dfSeparetedOps.reset_index(drop=True)
        dfSeparetedOps.drop_duplicates(inplace=True)
        return dfSeparetedOps

    def __defineDateCols(self, df):
        df = self.__convertColsToDatetime(df)
        df = self.__defineProcedureDate(df)

        df['procedure_end_datetime'] = df['op_chig_ende']
        df['procedure_end_date'] = df['op_chig_ende'].dt.date
        df['procedure_datetime'] = df['op_chig_beginn'].fillna(df['op_datum'])

        return df

    @staticmethod
    def __convertColsToDatetime(df):
        cols = ['op_chig_ende', 'op_naht', 'op_schnitt', 'op_chig_beginn', 'op_datum']
        for col in cols:
            df[col] = pd.to_datetime(df[col], format='mixed') # Todo: tomar cuidado aqui! Os dados já vem aware e logo nao devem passar por pd.to_datetime

        return df

    @staticmethod
    def __defineProcedureDate(df):
        df['procedure_date'] = (
            df['op_chig_ende']
            .fillna(df['op_naht'])
            .fillna(df['op_schnitt'])
            .fillna(df['op_chig_beginn'])
            .fillna(df['op_datum'])
        ).dt.date

        df = df.dropna(subset=['procedure_date'])
        return df

    @staticmethod
    def __addCustomMappingOPS(df):
        df['procedure_concept_id'] = df['op_ops'].apply(
            lambda x: conceptsIDs.get("procedure_concept_id_cardSurgery")
            if pd.notnull(x) and re.search(r'^5-(35|36|37)[a-zA-Z]?\.', x)
            else conceptsIDs.get("procedure_concept_id_surgery")
        )
        return df