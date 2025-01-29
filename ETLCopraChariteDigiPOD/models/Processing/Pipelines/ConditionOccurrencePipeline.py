from dataclasses import dataclass

import pandas as pd

from models.PipelineStarter.Ressources.ManualConceptIDs import conceptsIDs
from models.PipelineStarter.Ressources.Timezone import timezone
from models.Processing.Pipelines.BasePipeline import BasePipeline


@dataclass
class ConditionOccurrencePipeline(BasePipeline):
    def process(self):
        measurements_df = self._importOMOPTable('measurement')
        df = self.__processDelirOccurrences(measurements_df)
        df = self.__createDateColumns(df)

        df["condition_type_concept_id"] = conceptsIDs.get("condition_type_concept_id")
        processedDf = self._adaptSchema(df)

        return processedDf

    def __processDelirOccurrences(self, df):
        delirMeasurements = self.__filterDelirMeasurements(df)
        newDf = delirMeasurements.groupby('person_id').apply(self.__identifyDelirEpisodes).reset_index(drop=True)
        newDf['condition_source_value'] = 'Delirium'
        newDf['condition_concept_id'] = conceptsIDs.get('POD')

        newDf = self._createUniqueID(newDf, ['person_id', 'condition_start_datetime', 'condition_source_value'], self.idCol)

        return newDf

    @staticmethod
    def __filterDelirMeasurements(df):
        delirConcepts = [2000000012, 2000000013, 2000000014, 3662219, 2000000015, 2000000016, 2000000017, 2000000018, 2000000019]
        mask = (df['measurement_concept_id'].isin(delirConcepts) & df['value_as_concept_id'].isin([conceptsIDs.get("positive"), conceptsIDs.get("negative")]))
        delirMeasurements = df.loc[mask]
        return delirMeasurements

    @staticmethod
    def __identifyDelirEpisodes(df):
        df['delir_change'] = df['value_as_concept_id'] != df['value_as_concept_id'].shift(1)

        df['episode_start'] = (df['value_as_concept_id'] == conceptsIDs.get("positive")) & df['delir_change']

        df['episode_end'] = (df['value_as_concept_id'] == conceptsIDs.get("negative")) & df['delir_change'] & (df.index > df.index[0])

        starts = df[df['episode_start']][['person_id', 'measurement_datetime']]
        ends = df[df['episode_end']][['measurement_datetime']]

        starts = starts.rename(columns={'measurement_datetime': 'condition_start_datetime'})
        ends = ends.rename(columns={'measurement_datetime': 'condition_end_datetime'})

        starts = starts.reset_index(drop=True)
        ends = ends.reset_index(drop=True)

        episodes = pd.concat([starts, ends], axis=1)

        # Calcular e imputar valores em 'condition_end_datetime' com restrição de 5 dias
        today = pd.Timestamp('now', tz=timezone)
        mask = episodes['condition_end_datetime'].isna()

        # Imputar valores de 'condition_end_datetime' conforme a condição de 5 dias
        episodes.loc[mask, 'condition_end_datetime'] = episodes.loc[mask, 'condition_start_datetime'].apply(
            lambda start: today if (today - start).days <= 5 else start + pd.Timedelta(days=5)
        )

        return episodes

    @staticmethod
    def __createDateColumns(df):
        df['condition_start_datetime'] = pd.to_datetime(df['condition_start_datetime'], format='mixed')
        df['condition_start_date'] = df['condition_start_datetime'].dt.date
        df['condition_end_datetime'] = pd.to_datetime(df['condition_end_datetime'], format='mixed')
        df.loc[df['condition_end_datetime'].isna(), 'condition_end_datetime'] = df['condition_start_datetime']
        df['condition_end_date'] = df['condition_end_datetime'].dt.date
        return df
