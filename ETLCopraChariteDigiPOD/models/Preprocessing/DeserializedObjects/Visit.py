import pandas as pd
from dataclasses import dataclass
from typing import List, Dict

from models.Preprocessing.DeserializedObjects.BaseDeserializedObject import BaseDeserializedObject


@dataclass
class Visit:
    visit_data: dict[str, BaseDeserializedObject]

    def to_dict(self) -> Dict[str, pd.DataFrame]:
        output_dict = {}

        visit_metadata_df = self.visit_data['visitmetadata'].to_df()

        for att, att_value in self.visit_data.items():
            if att != 'visitmetadata' and att_value:
                if isinstance(att_value, list):
                    output_dict[att] = self._concat_with_identifiers(visit_metadata_df, [elem.to_df() for elem in att_value])
                elif not isinstance(att_value, list):
                    output_dict[att] = self._concat_with_identifiers(visit_metadata_df, [att_value.to_df()])

        return output_dict

    @staticmethod
    def _concat_with_identifiers(identifiers_df: pd.DataFrame, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        combined_df = pd.concat(dfs, ignore_index=True)

        identifiers_repeated = pd.concat([identifiers_df] * len(combined_df), ignore_index=True)

        result_df = pd.concat([identifiers_repeated, combined_df], axis=1)

        return result_df
