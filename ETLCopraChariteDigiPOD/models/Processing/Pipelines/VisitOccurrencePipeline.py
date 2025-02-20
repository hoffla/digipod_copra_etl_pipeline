import pandas as pd
from dataclasses import dataclass
from datetime import timedelta

from models.PipelineStarter.Ressources.ManualConceptIDs import conceptsIDs
from models.Processing.Pipelines.BasePipeline import BasePipeline


@dataclass
class VisitOccurrencePipeline(BasePipeline):
    def process(self) -> pd.DataFrame:
        if self.rawData:
            combined_df = self._collect_visit_data()
            combined_df_withPersonID = self._addPersonID(combined_df)
            visit_occurrence_df = self._importOMOPTable('visit_occurrence')

            updated_existing_visits = self._process_and_update_existing_patients(combined_df_withPersonID, visit_occurrence_df)
            new_visits = self._process_and_update_new_patients(combined_df_withPersonID, visit_occurrence_df)

            df = pd.concat([updated_existing_visits, new_visits], ignore_index=True)
            df = self._expand_patient_intervals_by_patient(df)
            df = self.__createDateColumns(df)
            df = self.__fillRequiredCols(df)
            df = self._createUniqueID(df, ['person_id', 'casenumber', 'visit_start_datetime'], self.idCol)
            df = self._adaptSchema(df)

            return df.drop_duplicates()

    def _collect_visit_data(self) -> pd.DataFrame:
        visit_data = [
            df[['visit_datetime', 'casenumber']]
            for df in self.rawData.values()
            if 'visit_datetime' in df.columns and 'casenumber' in df.columns
        ]
        combined_visit_data = pd.concat(visit_data, ignore_index=True).drop_duplicates()
        combined_visit_data["visit_datetime"] = combined_visit_data["visit_datetime"].dt.tz_convert("UTC")
        return combined_visit_data

    def _process_and_update_existing_patients(self, combined_df: pd.DataFrame, visit_occurrence_df: pd.DataFrame) -> pd.DataFrame:
        existing_patients_df = combined_df[combined_df['person_id'].isin(visit_occurrence_df['person_id'])]

        updated_visits = [
            self._adjust_intervals(existing_visits, patient_visits_in_db, adjust_weeks=1)
            for person_id, existing_visits in existing_patients_df.groupby('person_id')
            if (patient_visits_in_db := visit_occurrence_df[visit_occurrence_df['person_id'] == person_id]).empty is False
        ]

        return pd.concat(updated_visits, ignore_index=True) if updated_visits else pd.DataFrame()

    def _process_and_update_new_patients(self, combined_df: pd.DataFrame, visit_occurrence_df: pd.DataFrame) -> pd.DataFrame:
        new_patients_df = combined_df[~combined_df['person_id'].isin(visit_occurrence_df['person_id'])]

        updated_visits = [
            self._adjust_intervals(new_visits, adjust_weeks=4)
            for _, new_visits in new_patients_df.groupby('person_id')
        ]

        return pd.concat(updated_visits, ignore_index=True) if updated_visits else pd.DataFrame()

    def _adjust_intervals(self, new_visits: pd.DataFrame, existing_visits=pd.DataFrame(), adjust_weeks: int = 1) -> pd.DataFrame:
        min_visit, max_visit = self._extractMinMaxVals(new_visits, existing_visits)

        adjusted_start = min_visit - timedelta(weeks=adjust_weeks)
        adjusted_end = max_visit + timedelta(weeks=adjust_weeks)

        return pd.DataFrame({
            'person_id': [new_visits['person_id'].iloc[0]],
            'casenumber': [new_visits['casenumber'].iloc[0]],
            'visit_start_datetime': [adjusted_start],
            'visit_end_datetime': [adjusted_end]
        })

    @staticmethod
    def _extractMinMaxVals(new_visits, existing_visits) -> tuple:
        if not existing_visits.empty:
            min_visit = min(new_visits['visit_datetime'].min(), existing_visits['visit_start_datetime'].min())
            max_visit = max(new_visits['visit_datetime'].max(), existing_visits['visit_end_datetime'].max())
        else:
            min_visit = new_visits['visit_datetime'].min()
            max_visit = new_visits['visit_datetime'].max()

        return min_visit, max_visit

    @staticmethod
    def _expand_patient_intervals_by_patient2(df: pd.DataFrame) -> pd.DataFrame:
        expanded_rows = []

        # Agrupa o DataFrame por 'person_id' para expandir as linhas por paciente
        for person_id, group in df.groupby('person_id'):
            for _, row in group.iterrows():
                start_date = row['visit_start_datetime']
                end_date = row['visit_end_datetime']
                current_start = start_date

                # Expande intervalos em dias até `visit_end_datetime`
                while current_start <= end_date:
                    current_end = min(current_start.replace(hour=23, minute=59, second=59), end_date)

                    # Adiciona uma nova linha para o intervalo de um dia para o paciente atual
                    expanded_rows.append({
                        'person_id': person_id,
                        'casenumber': row['casenumber'],
                        'visit_start_datetime': current_start,
                        'visit_end_datetime': current_end
                    })

                    # Incrementa para o próximo dia
                    current_start = (current_start + timedelta(days=1)).replace(hour=0, minute=0, second=0)

        # Cria o DataFrame final com as linhas expandidas para todos os pacientes
        expanded_df = pd.DataFrame(expanded_rows)
        return expanded_df

    @staticmethod
    def _expand_patient_intervals_by_patient(df: pd.DataFrame) -> pd.DataFrame:
        df['daily_dates'] = df.apply(
            lambda row: pd.date_range(
                start=row['visit_start_datetime'].normalize(),
                end=row['visit_end_datetime'].normalize(),
                freq='D'
            ), axis=1
        )

        expanded_df = df.explode('daily_dates', ignore_index=True)

        expanded_df['visit_start_datetime'] = expanded_df['daily_dates'] + timedelta(hours=0, minutes=0, seconds=0)
        expanded_df['visit_end_datetime'] = expanded_df['daily_dates'] + timedelta(hours=23, minutes=59, seconds=59)

        return expanded_df[['person_id', 'casenumber', 'visit_start_datetime', 'visit_end_datetime']]

    @staticmethod
    def __createDateColumns(df):
        """
        Converte colunas datetime e cria as colunas de data associadas.
        """
        for datetimeCol, dateCol in zip(['visit_start_datetime', 'visit_end_datetime'], ['visit_start_date', 'visit_end_date']):
            df[datetimeCol] = pd.to_datetime(df[datetimeCol], utc=True, format='mixed')
            df[dateCol] = df[datetimeCol].dt.date
        return df

    @staticmethod
    def __fillRequiredCols(df):
        df["visit_type_concept_id"] = conceptsIDs.get("visit_type_concept_id")
        df['visit_concept_id'] = 9201
        df['visit_source_value'] = 'Synthetic Data'
        return df
