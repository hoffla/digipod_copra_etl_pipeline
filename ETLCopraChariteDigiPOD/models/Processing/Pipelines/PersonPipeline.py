from dataclasses import dataclass

import pandas as pd

from models.Processing.Pipelines.BasePipeline import BasePipeline


@dataclass
class PersonPipeline(BasePipeline):
    def process(self):
        if self.rawData:
            dfs = self._createDataframes()
            df = self.__mergeDataframes(dfs)
            df = self.__createBirthdayColumns(df)
            df = self.__removeDuplicateCols(df)
            df = self.__createUniqueID(df, ['record_id'], self.idCol)
            df = self._addOMOPConceptCols(df)

            self._renameCols(df, mapping={
                'geburtstag': 'birth_datetime',
                'person_source_value': 'record_id',
                'gender_source_value': 'geschlecht',
            })

            processedDf = self._adaptSchema(df)

            return processedDf

    def __mergeDataframes(self, dfs):
        geburtDf = dfs.get('einschluss_arm_2')
        geschlechtDf = dfs.get('properative_visite_arm_2')
        df = geburtDf.merge(geschlechtDf, on='record_id', how='left')
        return df

    def __createBirthdayColumns(self, df):
        df['geburtstag'] = pd.to_datetime(df['geburtstag'], format='mixed')
        df['day_of_birth'] = df.geburtstag.dt.day
        df['month_of_birth'] = df.geburtstag.dt.month
        df['year_of_birth'] = df.geburtstag.dt.year
        return df

    def __removeDuplicateCols(self, df):
        toRemoveCols = ['redcap_event_name', 'redcap_repeat_instrument', 'redcap_repeat_instance', 'redcap_data_access_group']
        df = self._colsToRemove(df, toRemoveCols, keepCopy=['redcap_data_access_group'])
        return df