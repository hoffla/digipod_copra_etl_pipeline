from dataclasses import dataclass

import pandas as pd

from models.PipelineStarter.Ressources.ManualConceptIDs import conceptsIDs
from models.Processing.Pipelines.BasePipeline import BasePipeline


measurementsMapping = {
    'praemed_asa': 'ASA Physical Status Classification', #TODO: fazer o asa
    'cog_minicog_sum': 'Mini-Cog©',
    'frailty': 'Fried Frailty Phenotype'
}

mappingsDelir = {
    'Delir': conceptsIDs.get("positive"),
    'Delir wahrscheinlich': conceptsIDs.get("positive"),
    'Kein Delir': conceptsIDs.get("negative"),
    'Unmöglich': conceptsIDs.get("undecided"),
    'Subsyndromales Delir': conceptsIDs.get("weakly positive"),
    'sehr streitlustig': conceptsIDs.get("sehr streitlustig"),
    'sehr agitiert': conceptsIDs.get("sehr agitiert"),
    'agitiert': conceptsIDs.get("agitiert"),
    'unruhig': conceptsIDs.get("unruhig"),
    #'aufmerksam, ruhig': conceptsIDs.get("aufmerksam, ruhig"),
    'schläfrig': conceptsIDs.get("schläfrig"),
    'leichte Sedierung': conceptsIDs.get("leichte Sedierung"),
    'mäßige Sedierung': conceptsIDs.get("mäßige Sedierung"),
    'tiefe Sedierung': conceptsIDs.get("tiefe Sedierung"),
    'nicht erweckbar': conceptsIDs.get("nicht erweckbar"),
            }


mappingsFrailty = {
    'Frail': conceptsIDs.get("frail"),
    'Pre-Frail': conceptsIDs.get("pre-frail"),
    'Non Frail': conceptsIDs.get("non-frail"),
}

mappingsASA = {
    "praemed_asa": conceptsIDs.get("asa_3"),
}


@dataclass
class MeasurementPipeline(BasePipeline):
    def process(self):
        if self.rawData:
            df = self.__processDelirMeasurements(self.rawData.get('deliriumscore', pd.DataFrame()))
            mini_cog = self.__processMiniCog(self.rawData.get('minicog', pd.DataFrame())) # quando for usar isso mudar o nome do dicionário e do key
            asa = self.__processASA(self.rawData.get('predispositionfactors', pd.DataFrame()))

            processedDf = self._adaptSchema(df, mini_cog, asa)
            processedDf = self.__createDateColumns(processedDf)
            processedDf["measurement_type_concept_id"] = conceptsIDs.get("measurement_type_concept_id")

            return processedDf

    def __processDelirMeasurements(self, df):
        if not df.empty:
            df = df[["delirtest_result2", "delirtest_result", "delirtest_datetime", "delirtest_typ", "visit_datetime", "casenumber"]].dropna()
            df = self._addPersonID(df)
            df = self.__addMappings(df, 'delirtest_result2', mappingsDelir)
            df = self._addColPerConvertion(df, 'delirtest_result', 'value_as_number', float)
            df = self._createUniqueID(df, ['person_id', 'delirtest_datetime', 'delirtest_typ'], self.idCol)  # Todo: lembrar de ter que colocar o person_id (pseudonym) aqui
            df = self._addOMOPConceptCols(df)
            df.rename(columns={'delirtest_datetime': 'measurement_datetime', 'delirtest_typ': 'measurement_source_value', 'delirtest_result2': 'value_source_value'}, inplace=True)

        return df  

    def __processMiniCog(self, df):
        if not df.empty:
            df = df[["cog_minicog_sum", "visit_datetime", "casenumber"]].dropna()
            df = self._addPersonID(df)
            df['source_column'] = "cog_minicog_sum"
            df = self._addColPerConvertion(df, 'cog_minicog_sum', 'value_as_number', int)
            df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'source_column'], self.idCol)  # Todo: lembrar de ter que colocar o person_id (pseudonym) aqui
            df = self._addOMOPConceptCols(df)
            df.rename(columns={'visit_datetime': 'measurement_datetime', 'cog_minicog_sum': 'value_source_value'}, inplace=True)
            df['measurement_source_value'] = measurementsMapping.get('cog_minicog_sum', 'Unknown Measurement')

            print("PRINTING FINAL MINI COG DF")
            print(df)

        return df
    
    def __processFrailty(self, df):
        if not df.empty:
            df = df[["frailty", "frailty_criteria", "visit_datetime", "casenumber"]].dropna()
            df = self._addPersonID(df)
            df = self._addColPerConvertion(df, 'frailty', 'value_as_number', int)
            df = self.__addMappings(df, 'frailty_criteria', mappingsFrailty)
            df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'frailty_criteria'], self.idCol)
            df = self._addOMOPConceptCols(df)
            df.rename(columns={'visit_datetime': 'measurement_datetime', 'frailty_criteria': 'value_source_value'}, inplace=True)
            df['measurement_source_value'] = measurementsMapping.get('frailty', 'Unknown Measurement')
            
        return df
    
    def __processASA(self, df):
        if "praemed_asa" in df.columns:
            df = df.loc[df["praemed_asa"]].dropna()

            if not df.empty:
                df = self._addPersonID(df)
                df["value_as_number"] = 3
                df["pramed_asa_name"] = "praemed_asa" 
                df = self.__addMappings(df, 'praemed_asa', mappingsASA)
                df = self._createUniqueID(df, ['person_id', 'visit_datetime', 'pramed_asa_name'], self.idCol)
                df = self._addOMOPConceptCols(df)
                df.rename(columns={'visit_datetime': 'measurement_datetime'}, inplace=True)

        return df
    
    @staticmethod
    def __addMappings(df, col, mapping):
        df['value_as_concept_id'] = df[col].replace(mapping)
        df['value_as_concept_id'] = pd.to_numeric(df['value_as_concept_id'], errors='coerce').fillna(0)

        return df

    @staticmethod
    def __createDateColumns(df):
        df = df.dropna(subset=['measurement_datetime'])
        df['measurement_date'] = df['measurement_datetime'].dt.date
        return df
