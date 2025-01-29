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

}


@dataclass
class ObservationPipeline(BasePipeline):
    '''
    Lembrar que alguns dados que estao ali em cima podem nao estar no xml!
    '''
    def process(self):
        if self.rawData:
            praeOp = self.__processPraeopObs(self.rawData)
            eeg = self.__processEEG(self.rawData.get("perioop")) # Todo: virá do peri, se houver
            dexPrev = self.__processDex(self.rawData.get('medikation_arm_2')) # Todo: ainda nao tenho nada de medicacao aqui -> deletar?

            df = pd.concat([eeg, praeOp, dexPrev])
            df = self.__createDateColumns(df)
            df["observation_type_concept_id"] = conceptsIDs.get("observation_type_concept_id")

            processedDf = self._adaptSchema(df)

            return processedDf

    def __processPraeopObs(self, df):
        '''
        TEREI AQUI QUE CONCATERNAR AS COLUNAS DOS DOIS DATAFRAMES -> "predispositionfactors" terá as colunas "praemed_rf_erhebung" & "praemed_rf_praedi" / "precipitatingfactors" terá as colunas "praemed_rf_erhebung" & "praemed_rf_praezi"
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

    def __processEEG(self, df):
        df = self.__addPersonID(df)

        dfsCols = [
            (['person_id', 'op_datum', 'op_eeg_used'], False),
            (['person_id', 'op_datum', 'op_eeg_bs_minuten'], True),
            (['person_id', 'op_datum', 'op_eeg_bs_ratio'], True),
        ]

        return self.__processDfs(df, dfsCols)

    def __processDex(self, df):
        df = self.__addPersonID(df)

        dfsCols = [
            (['person_id', 'medeinzel_datetime_show', 'medeinzel_dex_prev'], False),
        ]

        return self.__processDfs(df, dfsCols)

    def __processDfs(self, df, dfsCols):
        praopObs = pd.DataFrame(columns=[self.idCol, 'person_id'])
        for dfCols, asNumber in dfsCols:
            newDf = self.__createDataframe(df, dfCols, asNumber)
            if isinstance(newDf, pd.DataFrame):
                praopObs = pd.concat([praopObs, newDf])

        return praopObs

    def __createDataframe(self, df, cols, asNumber) -> pd.DataFrame:
        newDf = self.__extractColumnsAndDropMissings(df, cols)
        if not newDf.empty:
            newDf = self.__addNewColums(newDf, cols, asNumber)
            newDf = self.__createUniqueID(newDf, ['person_id', cols[1], 'observation_source_value'], self.idCol)
            newDf = self._addOMOPConceptCols(newDf)
            newDf.rename(columns={cols[1]: 'observation_datetime', cols[2]: 'value_source_value'}, inplace=True)

            return newDf

    @staticmethod
    def __extractColumnsAndDropMissings(df, cols):
        return df[cols].replace('', pd.NA).dropna()

    @staticmethod
    def __addNewColums(newDf, cols, asNumber=False):
        newDf['observation_source_value'] = observationMapping.get(cols[2], 'Unknown Observation')

        if asNumber:
            newDf['value_as_number'] = newDf[cols[2]]
        else:
            newDf[cols[2]] = newDf[cols[2]].astype(str)
            if cols[2] == 'praemed_asa':
                newDf['value_as_concept_id'] = newDf[cols[2]].replace(
                    {
                        '1': conceptsIDs.get("asa_1"), '2': conceptsIDs.get("asa_2"), '3': conceptsIDs.get("asa_3"),
                        '4': conceptsIDs.get("asa_4"), '5': conceptsIDs.get("asa_5"), '6': conceptsIDs.get("asa_6")
                    })
            else:
                newDf['value_as_concept_id'] = newDf[cols[2]].replace(
                    {'0': conceptsIDs.get("absent"), '1': conceptsIDs.get("present")})

        return newDf

    @staticmethod
    def __createDateColumns(df):
        df = df.dropna(subset=['observation_datetime'])
        df['observation_date'] = df['observation_datetime'].dt.date
        return df
