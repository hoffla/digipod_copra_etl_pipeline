from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import os
from typing import List
import pandas as pd
from models.Preprocessing.Utils.XMLNavigator import XMLNavigator
from models.PipelineStarter.event import post_event
from models.Utils.SQLInteractor import getOrCreateSQLEngine

messages = {
            "en": {
                "subject": "Cases with PatID Found but Missing Casenumber",
                "body": """The following cases do not have a corresponding casenumber but have a PatID present in the database:

        {unmatched_cases}

Please verify if these casenumbers should be added to our REDCap! If so, please add them."""
            },
            "de": {
                "subject": "Neue Fallnummer zu bereits eingeschlossenen Patienten",
                "body": """Es wurde bei den folgenden Patienten neue Fallnummer identizifiert, welche in REDCap nicht vorhanden sind:

        {unmatched_cases}

Bitte prüfen Sie, ob diese Fallnummer zu unserem REDCap hinzugefügt werden sollen! Falls ja, bitte fügen Sie sie hinzu."""
            }
        }


@dataclass
class PatientFilter:
    sqlEngine: object = field(init=False)
    schema: str = 'cds_cdm'
    hash_file: str = os.path.join(os.getenv('BASEPATH'), os.getenv('PROJECTDIRECTORY'), os.getenv('RESSOURCES_DIR'), 'sent_casenumbers.json')

    def __post_init__(self):
        self.sqlEngine = getOrCreateSQLEngine()

        if not os.path.isfile(self.hash_file):
            with open(self.hash_file, 'w') as file:
                json.dump({}, file)

    def filterXMLFiles(self, xml_files: List[str]) -> tuple[List[str], List[str]]:
        patient_data = self._extract_patient_data(xml_files)

        fallnummer_set = [int(casenumber) for casenumber in patient_data["casenumber"].unique()]
        patient_id_set = [int(patient_id) for patient_id in patient_data["patient_id"].dropna().unique()]

        tableCasenumber_matches = self._fetch_casenumber_mapping(fallnummer_set)
        tablePatid_matches = self._fetch_patientID_mapping(patient_id_set)

        new_casenumber_xml_files = self._handle_unmatched_cases(patient_data, tableCasenumber_matches, tablePatid_matches)
        filtered_xml_files = self._handle_matched_cases(patient_data, tableCasenumber_matches)
        no_digi_xml_files = self._handle_no_digi_pats(patient_data, filtered_xml_files, new_casenumber_xml_files)

        return filtered_xml_files, new_casenumber_xml_files, no_digi_xml_files

    def getPersonID(self, df) -> pd.DataFrame:
        casenumbers = df['casenumber'].unique()
        casenumber_info = self._fetch_casenumber_mapping(casenumbers, columns=['casenumber', 'pseudonym', 'person_id'], distinct=True)

        if casenumber_info.empty:
            raise ValueError("No corresponding casenumbers where found on casenumber_mappings.")

        merged_df = df.merge(
            casenumber_info,
            on='casenumber',
            how='left'
        )

        return merged_df

    @staticmethod
    def _extract_patient_data(xml_files: List[str]) -> pd.DataFrame:
        data = []

        for xml_file in xml_files:
            navigator = XMLNavigator(xml_file)
            casenumber = navigator.get_element_value(navigator.find_element('.//EINWILLIGUNGSSTATUS/FALNR'), 'VALUE')
            patID = navigator.get_element_value(navigator.find_element('.//MAIN_DOC/MAIN_DOC_METADATA/PATNR'), 'VALUE')
            data.append({"xml_file": xml_file, "casenumber": int(casenumber), "patient_id": int(patID)})

        return pd.DataFrame(data, columns=["xml_file", "casenumber", "patient_id"])

    def _fetch_casenumber_mapping(self, casenumber_set, **kwargs) -> pd.DataFrame:
        filterCasenumber = [('casenumber', 'IN', tuple(casenumber_set))]
        casenumberMatches = post_event('importTable', 'casenumber_mappings', self.sqlEngine, schema=self.schema, where=filterCasenumber, **kwargs)

        return casenumberMatches

    def _fetch_patientID_mapping(self, patient_id_set, **kwargs) -> pd.DataFrame:
        filterPatID = [('patient_id', 'IN', tuple(patient_id_set))]
        patidMatches = post_event('importTable', 'casenumber_mappings', self.sqlEngine, schema=self.schema, where=filterPatID, **kwargs)

        return patidMatches
    
    def _handle_unmatched_cases(self, patient_data, tableCasenumber_matches, tablePatid_matches):
        unmatched_cases = self._identify_unmatched_cases(patient_data, tableCasenumber_matches, tablePatid_matches)
        unmatched_cases_perEmail = unmatched_cases[~unmatched_cases["casenumber"].apply(self._is_casenumber_sent)].drop_duplicates()

        if not unmatched_cases_perEmail.empty:
            self._send_email_with_unmatched_cases(unmatched_cases_perEmail)
            unmatched_cases_perEmail["casenumber"].apply(self._mark_casenumber_as_sent)

        return unmatched_cases["xml_file"].drop_duplicates().tolist()

    def _is_casenumber_sent(self, casenumber: int) -> bool:
        casenumber_hash = hashlib.sha256(str(casenumber).encode()).hexdigest()
        with open(self.hash_file, 'r') as file:
            sent_cases = json.load(file)
        return casenumber_hash in sent_cases
        

    def _mark_casenumber_as_sent(self, casenumber: int):
        casenumber_hash = hashlib.sha256(str(casenumber).encode()).hexdigest()
        with open(self.hash_file, 'r') as file:
            sent_cases = json.load(file)
        
        sent_cases[casenumber_hash] = datetime.now()

        with open(self.hash_file, 'w') as file:
            json.dump(sent_cases, file, indent=4)

    def _identify_unmatched_cases(self, patient_data, casenumber_matches, patid_matches) -> pd.DataFrame:
        mask = ~patient_data["casenumber"].isin(casenumber_matches["casenumber"]) & patient_data["patient_id"].isin(patid_matches["patient_id"])
        unmatched_cases = self._filter_table(patient_data[mask], patid_matches, on='patient_id')
        return unmatched_cases

    @staticmethod
    def _send_email_with_unmatched_cases(unmatched_cases):
        unmatched_cases_str = unmatched_cases[['pseudonym', 'casenumber']].to_string(index=False) 
        subjectGerman = messages["de"]["subject"]
        bodyGermanFormatted = messages["de"]["body"].format(unmatched_cases=unmatched_cases_str)

        post_event('sendEmail', subjectGerman, bodyGermanFormatted, 'RECIPIENTS_EMAIL_DIGIPOD', onlyIntern=True) 

    def _handle_matched_cases(self, patient_data, tableCasenumber_matches):
        matched_cases = self._filter_table(patient_data, tableCasenumber_matches)
        self._update_missing_patient_ids(matched_cases)
        return matched_cases["xml_file"].drop_duplicates().tolist()

    @staticmethod
    def _filter_table(patient_data, tableCasenumber_matches, on="casenumber") -> pd.DataFrame:
        return patient_data.merge(
            tableCasenumber_matches,
            on=on,
            how="inner"
        ).rename(columns={'casenumber_x': 'casenumber'})

    def _update_missing_patient_ids(self, filtered_data: pd.DataFrame):
        tableCols = ['id', 'pseudonym', 'casenumber', 'person_id', 'patient_id']
        filtered_data.rename(columns={"patient_id_x": "patient_id"}, inplace=True)
        missing_patient_ids = filtered_data.loc[filtered_data["patient_id_y"].isna(), tableCols]
        missing_patient_ids = missing_patient_ids.drop_duplicates(subset=['id'])
        if not missing_patient_ids.empty:
            post_event('updateTable', missing_patient_ids, 'casenumber_mappings', schema=self.schema)


    def _handle_no_digi_pats(self, patient_data, filtered_xml_files, new_casenumber_xml_files):
        noDigiPats = patient_data.loc[~patient_data['xml_file'].isin(filtered_xml_files + new_casenumber_xml_files)]
        return noDigiPats['xml_file'].drop_duplicates().to_list()
