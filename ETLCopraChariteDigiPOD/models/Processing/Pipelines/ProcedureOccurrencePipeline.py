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
            cog = self.__processCognition(self.rawData.get('cognition', pd.DataFrame()))
            anxiety = self.__processAnxiety(self.rawData.get('anxiety', pd.DataFrame()))
            mobi = self.__processMobi(self.rawData.get('mobilization', pd.DataFrame()))
            nutri = self.__processNutri(self.rawData.get('nutrition', pd.DataFrame()))
            dispha = self.__processDispha(self.rawData.get('dysphagia', pd.DataFrame()))
            mouth = self.__processMouth(self.rawData.get('mouthhygiene', pd.DataFrame()))

            processedDf = self._adaptSchema(cog, anxiety, mobi, nutri, dispha, mouth)
            processedDf["procedure_type_concept_id"] = conceptsIDs.get("procedure_type_concept_id")
            processedDf = self.__createDateColumns(processedDf)

            return processedDf
        
    def __processCognition(self, df):
        if not df.empty:
            columns_to_expand = ["kog_erfolgt", "orientierung_erfolgt", "kommuni_erfolgt", "circrhy_wie___1"]
            df = df[["visit_datetime", "casenumber"] + columns_to_expand].dropna()
            df = self.__reshape_dataframe(df, columns_to_expand)
            df = df[df['source_column'] != False]
            df = self._addPersonID(df)
            df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'source_column'], self.idCol)
            df = self._addOMOPConceptCols(df, localJoin='source_code')
            df = self.__addMappings(df, 'source_column', procedureMapping)
            df.rename(columns={'visit_datetime': 'procedure_datetime'}, inplace=True)
            
        return df
    
    def __processAnxiety(self, df):
        if not df.empty:
            df = df[["angst_bewaltigung_typ___1", "visit_datetime", "casenumber"]].dropna()
            df = df[df["angst_bewaltigung_typ___1"] != False]
            df = self._addPersonID(df)
            df['source_column'] = "angst_bewaltigung_typ___1"
            df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'source_column'], self.idCol)
            df = self._addOMOPConceptCols(df)
            df = self.__addMappings(df, 'source_column', procedureMapping)
            df.rename(columns={'visit_datetime': 'procedure_datetime'}, inplace=True)

        return df
    

    def __processMobi(self, df):
        if not df.empty:
            df = df[["mobil_erfolgt", "visit_datetime", "casenumber"]].dropna()
            df = df[df['mobil_erfolgt'] != False]
            df = self._addPersonID(df)
            df['source_column'] = "mobil_erfolgt"
            df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'source_column'], self.idCol)
            df['value_as_concept_id'] = df["mobil_erfolgt"].map({True: conceptsIDs.get("present"), False: conceptsIDs.get("absent")})
            df = self._addOMOPConceptCols(df)
            df = self.__addMappings(df, 'source_column', procedureMapping)
            df.rename(columns={'visit_datetime': 'procedure_datetime'}, inplace=True)

        return df
    

    def __processNutri(self, df):
        if not df.empty:
            df = df[["nutri_erfolgt", "visit_datetime", "casenumber"]].dropna()
            df = df[df['nutri_erfolgt'] != False]
            df = self._addPersonID(df)
            df['source_column'] = "nutri_erfolgt"
            df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'source_column'], self.idCol)
            df['value_as_concept_id'] = df["nutri_erfolgt"].map({True: conceptsIDs.get("present"), False: conceptsIDs.get("absent")})
            df = self._addOMOPConceptCols(df)
            df = self.__addMappings(df, 'source_column', procedureMapping)
            df.rename(columns={'visit_datetime': 'procedure_datetime'}, inplace=True)

        return df
    

    def __processDispha(self, df):
        if not df.empty:
            columns_to_expand = ["schluck_behandlung", "schluck_nutri_umstellung"]
            df = df[["visit_datetime", "casenumber"] + columns_to_expand].dropna()
            df = self.__reshape_dataframe(df, columns_to_expand)
            df = df[df['source_column'] != False]
            df = self._addPersonID(df)
            df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'source_column'], self.idCol)
            df['value_as_concept_id'] = df["value"].map({True: conceptsIDs.get("present"), False: conceptsIDs.get("absent")})
            df = self._addOMOPConceptCols(df, localJoin='source_code')
            df = self.__addMappings(df, 'source_column', procedureMapping)
            df.rename(columns={'visit_datetime': 'procedure_datetime'}, inplace=True)

        return df
    
    def __processMouth(self, df):
        if not df.empty:
            df = df[["mundhyg_erfolgt", "visit_datetime", "casenumber"]].dropna()
            df = self._addPersonID(df)
            df['source_column'] = "mundhyg_erfolgt"
            df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'source_column'], self.idCol)
            df['value_as_concept_id'] = df["mundhyg_erfolgt"].map({True: conceptsIDs.get("present"), False: conceptsIDs.get("absent")})
            df = self._addOMOPConceptCols(df)
            df = self.__addMappings(df, 'source_column', procedureMapping)
            df.rename(columns={'visit_datetime': 'procedure_datetime'}, inplace=True)

        return df
    
    @staticmethod
    def __reshape_dataframe(df, columns_to_expand):
        return df.melt(
            id_vars=[col for col in df.columns if col not in columns_to_expand], 
            value_vars=columns_to_expand, 
            var_name="source_column", 
            value_name="value"
            )
    
    @staticmethod
    def __createDateColumns(df):
        df = df.dropna(subset=['procedure_datetime'])
        df['procedure_end_datetime'] = df['procedure_datetime']
        df['procedure_end_date'] = df['procedure_datetime'].dt.date
        df['procedure_date'] = df['procedure_datetime'].dt.date
        
        return df
    
    @staticmethod
    def __addMappings(df, col, mapping):
        df['procedure_source_value'] = df[col].replace(mapping)
        return df