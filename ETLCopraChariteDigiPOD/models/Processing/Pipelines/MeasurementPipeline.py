from dataclasses import dataclass

import pandas as pd

from models.PipelineStarter.Ressources.ManualConceptIDs import conceptsIDs
from models.Processing.Pipelines.BasePipeline import BasePipeline


measurementsMapping = {
    'charlson_sum': 'Charlson Comorbidity Index',
    'cog_minicog_sum': 'Mini-Cog©',
    'cog_moca_sum': 'Montreal-Cognitive-Assessment',
    'cog_acer_sum': "Addenbrooke's Cognitive Examination",
    'cog_mmse_sum': 'Mini-Mental State Examination',
}


@dataclass
class MeasurementPipeline(BasePipeline):
    def process(self):
        if self.rawData:
            df = self.__processDelirMeasurements(self.rawData.get('deliriumscore'))
            #praeopMeasurements = self.__processPraeopMeasurements(dfs.get('properative_visite_arm_2')) # quando for usar isso mudar o nome do dicionário e do key
            #dataframe = pd.concat([delirMeasurements, praeopMeasurements])
            df = self.__createDateColumns(df)

            df["measurement_type_concept_id"] = conceptsIDs.get("measurement_type_concept_id")
            processedDf = self._adaptSchema(df)

            return processedDf

    def __processDelirMeasurements(self, df):
        if not df.empty:
            df = self._addPersonID(df)
            df = self.__addMappings(df)
            df = self._addColPerConvertion(df, 'delirtest_result', 'value_as_number', float)
            df = self._createUniqueID(df, ['person_id', 'delirtest_datetime', 'delirtest_typ'], self.idCol)  # Todo: lembrar de ter que colocar o person_id (pseudonym) aqui
            df = self._addOMOPConceptCols(df)
            df.rename(columns={'delirtest_datetime': 'measurement_datetime', 'delirtest_typ': 'measurement_source_value', 'delirtest_result2': 'value_source_value'}, inplace=True)

            return df
        return pd.DataFrame()

    @staticmethod
    def __addMappings(df):
        df['value_as_concept_id'] = df['delirtest_result2'].replace( # todo tenho que pegar todos os possíveis resultados (incluir aqui os pontos do RASS!!) Depois adicionar na funcao quero colocar None os que nao foram pegos!!!!
            {
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
        )
        
        df['value_as_concept_id'] = pd.to_numeric(df['value_as_concept_id'], errors='coerce').fillna(0)

        return df

    def __processPraeopMeasurements(self, df):
        df = self._addPersonID(df)
        
        dfsCols = [
            ['person_id', 'charlson_datetime', 'charlson_sum'],
            ['person_id', 'cog_minicog_datetime', 'cog_minicog_sum'],
            ['person_id', 'cog_second_test_datetime', 'cog_moca_sum'],
            ['person_id', 'cog_second_test_datetime', 'cog_acer_sum'],
            ['person_id', 'cog_second_test_datetime', 'cog_mmse_sum']
        ]

        praopMeasurements = pd.DataFrame(columns=[self.idCol, 'person_id'])
        for dfCols in dfsCols:
            newDf = self.__createDataframe(df, dfCols)
            if isinstance(newDf, pd.DataFrame):
                praopMeasurements = pd.concat([praopMeasurements, newDf])

        return praopMeasurements

    def __createDataframe(self, df, cols) -> pd.DataFrame:
        newDf = self.__extractColumnsAndDropMissings(df, cols)
        if not newDf.empty:
            newDf = self.__addNewColums(newDf, cols)
            newDf = self._createUniqueID(newDf, ['person_id', cols[1], 'measurement_source_value'], self.idCol)
            newDf = self._addOMOPConceptCols(newDf)
            newDf.rename(columns={cols[1]: 'measurement_datetime', cols[2]: 'value_source_value'}, inplace=True)

            return newDf

    @staticmethod
    def __extractColumnsAndDropMissings(df, cols):
        return df[cols].replace('', pd.NA).dropna()

    @staticmethod
    def __addNewColums(newDf, cols):
        newDf['measurement_source_value'] = measurementsMapping.get(cols[2], 'Unknown Measurement')
        newDf['value_as_number'] = newDf[cols[2]]

        return newDf

    @staticmethod
    def __createDateColumns(df):
        df = df.dropna(subset=['measurement_datetime'])
        df['measurement_date'] = df['measurement_datetime'].dt.date
        return df


