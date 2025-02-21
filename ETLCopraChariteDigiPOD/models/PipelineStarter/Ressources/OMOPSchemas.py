import copy
from dataclasses import dataclass

import pandas as pd

OMOPSchemas = {
    "concept": {
        "concept_id": "object",
        "concept_name": "object",
        "domain_id": "object",
        "vocabulary_id": "object",
        "concept_class_id": "object",
        "standard_concept": "object",
        "concept_code": "object",
        "valid_start_date": "datetime64[ns]",
        "valid_end_date": "datetime64[ns]",
        "invalid_reason": "object"
    },
    "concepts_relationship": {
        "concept_id_1": "object",
        "concept_id_2": "object",
        "relationship_id": "object",
        "valid_start_date": "datetime64[ns]",
        "valid_end_date": "datetime64[ns]",
        "invalid_reason": "object"
    },
    "vocabularies": {
        "vocabulary_id": "object",
        "vocabulary_name": "object",
        "vocabulary_reference": "object",
        "vocabulary_version": "object",
        "vocabulary_concept_id": "object"
    },
    "sourceToConcept": {
        "source_code": "object",
        "source_concept_id": "object",
        "source_vocabulary_id": "object",
        "source_code_description": "object",
        "target_concept_id": "object",
        "valid_start_date": "datetime64[ns]",
        "valid_end_date": "datetime64[ns]",
        "invalid_reason": "object"
    },
    "localToLocal": {
        "Variabelname": "object",
        "mapsTo": "object"
    },
    "domainID": {
        "domain_id": "object",
        "CDM_Table": "object",
        "Field": "object"
    },
    "care_site": {
        "care_site_id": pd.Int64Dtype(),
        "care_site_name": "object",
        "place_of_service_concept_id": "object",
        "location_id": pd.Int64Dtype(),
        "care_site_source_value": "object",
        "place_of_service_source_value": "object"
    },
}


tableSchemas = {
    "person": {
        "person_id": pd.Int64Dtype(),
        "gender_concept_id": pd.Int64Dtype(),
        "year_of_birth": pd.Int32Dtype(),
        "month_of_birth": pd.Int32Dtype(),
        "day_of_birth": pd.Int32Dtype(),
        "birth_datetime": "datetime64[ns]",
        "race_concept_id": pd.Int32Dtype(),
        "ethnicity_concept_id": pd.Int32Dtype(),
        "location_id": pd.Int32Dtype(),
        "provider_id": pd.Int32Dtype(),
        "care_site_id": pd.Int32Dtype(),
        "person_source_value": "object",
        "gender_source_value": "object",
        "gender_source_concept_id": pd.Int32Dtype(),
        "race_source_value": "object",
        "race_source_concept_id": pd.Int32Dtype(),
        "ethnicity_source_value": "object",
        "ethnicity_source_concept_id": pd.Int32Dtype()
    },
    "visit_occurrence": {
        "visit_occurrence_id": pd.Int64Dtype(),
        "person_id": pd.Int64Dtype(),
        "visit_concept_id": pd.Int64Dtype(),
        "visit_start_date": "datetime64[ns]",
        "visit_start_datetime": "datetime64[ns]",
        "visit_end_date": "datetime64[ns]",
        "visit_end_datetime": "datetime64[ns]",
        "visit_type_concept_id": pd.Int64Dtype(),
        "provider_id": pd.Int32Dtype(),
        "care_site_id": pd.Int32Dtype(),
        "admitted_from_concept_id": pd.Int64Dtype(),
        "discharged_to_concept_id": pd.Int64Dtype(),
        "preceding_visit_occurrence_id": pd.Int64Dtype(),
        "visit_source_value": "object",
        "visit_source_concept_id": pd.Int64Dtype(),
        "admitted_from_source_value": "object",
        "discharged_to_source_value": "object"
    },
    "visit_detail": {
        "visit_detail_id": pd.Int64Dtype(),
        "person_id": pd.Int64Dtype(),
        "visit_detail_concept_id": pd.Int64Dtype(),
        "visit_start_datetime": "datetime64[ns]",
        "visit_end_datetime": "datetime64[ns]",
        "visit_type_concept_id": pd.Int64Dtype(),
        "provider_id": pd.Int32Dtype(),
        "care_site_id": pd.Int32Dtype(),
        "admitted_from_source_concept_id": pd.Int64Dtype(),
        "discharged_to_concept_id": pd.Int64Dtype(),
        "preceding_visit_detail_id": pd.Int64Dtype(),
        "visit_source_value": "object",
        "visit_source_concept_id": pd.Int32Dtype(),
        "admitted_from_source_value": "object",
        "discharged_to_source_value": "object",
        "visit_detail_parent_id": pd.Int64Dtype(),
        "visit_occurrence_id": pd.Int64Dtype()
    },
    "death": {
        "person_id": pd.Int64Dtype(),
        "death_date": "datetime64[ns]",
        "death_datetime": "datetime64[ns]",
        "death_type_concept_id": pd.Int32Dtype(),
        "cause_concept_id": pd.Int32Dtype(),
        "cause_source_value": "object",
        "cause_source_concept_id": pd.Int32Dtype()
    },
    "care_site": {
        "care_site_id": pd.Int32Dtype(),
        "care_site_name": "object",
        "place_of_service_concept_id": pd.Int32Dtype(),
        "location_id": pd.Int32Dtype(),
        "care_site_source_value": "object",
        "place_of_service_source_value": "object"
    },
    "procedure_occurrence": {
        "procedure_occurrence_id": pd.Int64Dtype(),
        "person_id": pd.Int64Dtype(),
        "procedure_concept_id": pd.Int64Dtype(),
        "procedure_date": "datetime64[ns]",
        "procedure_datetime": "datetime64[ns]",
        "procedure_end_date": "datetime64[ns]",
        "procedure_end_datetime": "datetime64[ns]",
        "procedure_type_concept_id": pd.Int64Dtype(),
        "modifier_concept_id": pd.Int32Dtype(),
        "quantity": pd.Int32Dtype(),
        "provider_id": pd.Int32Dtype(),
        "visit_occurrence_id": pd.Int64Dtype(),
        "visit_detail_id": pd.Int64Dtype(),
        "procedure_source_value": "object",
        "procedure_source_concept_id": pd.Int32Dtype(),
        "modifier_source_value": "object"
    },
    "measurement": {
        "measurement_id": pd.Int64Dtype(),
        "person_id": pd.Int64Dtype(),
        "measurement_concept_id": pd.Int64Dtype(),
        "measurement_date": "datetime64[ns]",
        "measurement_datetime": "datetime64[ns]",
        "measurement_type_concept_id": pd.Int64Dtype(),
        "operator_concept_id": pd.Int64Dtype(),
        "value_as_number": "float64",
        "value_as_concept_id": pd.Int64Dtype(),
        "unit_concept_id": pd.Int64Dtype(),
        "range_low": "float64",
        "range_high": "float64",
        "provider_id": pd.Int32Dtype(),
        "visit_occurrence_id": pd.Int64Dtype(),
        "measurement_source_value": "object",
        "measurement_source_concept_id": pd.Int64Dtype(),
        "unit_source_value": "object",
        "value_source_value": "object"
    },
    "observation": {
        "observation_id": pd.Int64Dtype(),
        "person_id": pd.Int64Dtype(),
        "observation_concept_id": pd.Int64Dtype(),
        "observation_date": "datetime64[ns]",
        "observation_datetime": "datetime64[ns]",
        "observation_type_concept_id": pd.Int64Dtype(),
        "value_as_number": "float64",
        "value_as_string": "object",
        "value_as_concept_id": pd.Int64Dtype(),
        "qualifier_concept_id": pd.Int32Dtype(),
        "unit_concept_id": pd.Int32Dtype(),
        "provider_id": pd.Int32Dtype(),
        "visit_occurrence_id": pd.Int64Dtype(),
        "observation_source_value": "object",
        "observation_source_concept_id": pd.Int64Dtype(),
        "unit_source_value": "object",
        "qualifier_source_value": "object"
    },
    "condition_occurrence": {
        "condition_occurrence_id": pd.Int64Dtype(),
        "person_id": pd.Int64Dtype(),
        "condition_concept_id": pd.Int64Dtype(),
        "condition_start_date": "datetime64[ns]",
        "condition_start_datetime": "datetime64[ns]",
        "condition_end_date": "datetime64[ns]",
        "condition_end_datetime": "datetime64[ns]",
        "condition_type_concept_id": pd.Int32Dtype(),
        "stop_reason": "object",
        "provider_id": pd.Int32Dtype(),
        "visit_occurrence_id": pd.Int64Dtype(),
        "condition_status_concept_id": pd.Int64Dtype(),
        "condition_source_concept_id": pd.Int64Dtype(),
        "condition_source_value": "object",
        "condition_status_source_value": "object"
    },
    "drug_exposure": {
        "drug_exposure_id": pd.Int64Dtype(),
        "person_id": pd.Int64Dtype(),
        "drug_concept_id": pd.Int32Dtype(),
        "drug_exposure_start_date": "datetime64[ns]",
        "drug_exposure_start_datetime": "datetime64[ns]",
        "drug_exposure_end_date": "datetime64[ns]",
        "drug_exposure_end_datetime": "datetime64[ns]",
        "drug_type_concept_id": pd.Int64Dtype(),
        "stop_reason": "object",
        "refills": pd.Int32Dtype(),
        "quantity": "float64",
        "days_supply": pd.Int32Dtype(),
        "route_concept_id": pd.Int32Dtype(),
        "effective_drug_dose": "float64",
        "dose_unit_concept_id": pd.Int64Dtype(),
        "lot_number": "object",
        "provider_id": pd.Int64Dtype(),
        "visit_occurrence_id": pd.Int64Dtype(),
        "drug_source_value": "object",
        "drug_source_concept_id": pd.Int64Dtype(),
        "route_source_value": "object",
        "dose_unit_source_value": "object"
    },
    "device_exposure": {
        "device_exposure_id": pd.Int64Dtype(),
        "person_id": pd.Int64Dtype(),
        "device_concept_id": pd.Int64Dtype(),
        "device_exposure_start_date": "datetime64[ns]",
        "device_exposure_start_datetime": "datetime64[ns]",
        "device_exposure_end_date": "datetime64[ns]",
        "device_exposure_end_datetime": "datetime64[ns]",
        "device_type_concept_id": pd.Int64Dtype(),
        "unique_device_id": "object",
        "quantity": pd.Int32Dtype(),
        "provider_id": pd.Int32Dtype(),
        "visit_occurrence_id": pd.Int64Dtype(),
        "device_source_value": "object",
        "device_source_concept_id": pd.Int64Dtype()
    },
    "casenumber_mappings": {
        "id": pd.Int64Dtype(),
        "pseudonym": "object",
        "casenumber": pd.Int64Dtype(),
        "person_id": pd.Int64Dtype(),
        "patient_id": pd.Int64Dtype(),
    },
}

OMOPColumns = {
    'person': [
        "person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth",
        "birth_datetime", "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id",
        "care_site_id", "person_source_value", "gender_source_value", "gender_source_concept_id",
        "race_source_value", "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"
    ],
    'visit_occurrence': [
        "visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date", "visit_start_datetime",
        "visit_end_date", "visit_end_datetime", "visit_type_concept_id", "provider_id", "care_site_id",
        "admitted_from_concept_id", "discharged_to_concept_id", "preceding_visit_occurrence_id", "visit_source_value",
        "visit_source_concept_id", "admitted_from_source_value", "discharged_to_source_value"
    ],
    'visit_detail': [
        "visit_detail_id", "person_id", "visit_detail_concept_id", "visit_start_datetime",
        "visit_end_datetime", "visit_type_concept_id", "provider_id", "care_site_id",
        "admitted_from_concept_id", "discharged_to_concept_id", "preceding_visit_detail_id",
        "visit_source_value", "visit_source_concept_id", "admitted_from_source_value", "discharged_to_source_value",
        "visit_detail_parent_id", "visit_occurrence_id"
    ],
    'death': [
        "person_id", "death_date", "death_datetime", "death_type_concept_id", "cause_concept_id",
        "cause_source_value", "cause_source_concept_id"
    ],
    'care_site': [
        "care_site_id", "care_site_name", "place_of_service_concept_id",
        "location_id", "care_site_source_value", "place_of_service_source_value"
    ],
    'procedure_occurrence': [
        "procedure_occurrence_id", "person_id", "procedure_concept_id",
        "procedure_date", "procedure_datetime", "procedure_end_date",
        "procedure_end_datetime", "procedure_type_concept_id",
        "modifier_concept_id", "quantity", "provider_id", "visit_occurrence_id",
        "visit_detail_id", "procedure_source_value", "procedure_source_concept_id", "modifier_source_value"
    ],
    'measurement': [
        "measurement_id", "person_id", "measurement_concept_id", "measurement_date",
        "measurement_datetime", "measurement_type_concept_id", "operator_concept_id",
        "value_as_number", "value_as_concept_id", "unit_concept_id", "range_low",
        "range_high", "provider_id", "visit_occurrence_id", "measurement_source_value",
        "measurement_source_concept_id", "unit_source_value", "value_source_value"
    ],
    'observation': [
        "observation_id", "person_id", "observation_concept_id",
        "observation_date", "observation_datetime", "observation_type_concept_id",
        "value_as_number", "value_as_string", "value_as_concept_id",
        "qualifier_concept_id", "unit_concept_id", "provider_id",
        "visit_occurrence_id", "observation_source_value", "observation_source_concept_id",
        "unit_source_value", "qualifier_source_value"
    ],
    'condition_occurrence': [
        "condition_occurrence_id", "person_id", "condition_concept_id",
        "condition_start_date", "condition_start_datetime", "condition_end_date",
        "condition_end_datetime", "condition_type_concept_id", "stop_reason",
        "provider_id", "visit_occurrence_id", "condition_status_concept_id",
        "condition_source_concept_id", "condition_source_value", "condition_status_source_value"
    ],
    'drug_exposure': [
        "drug_exposure_id", "person_id", "drug_concept_id", "drug_exposure_start_date",
        "drug_exposure_start_datetime", "drug_exposure_end_date", "drug_exposure_end_datetime",
        "drug_type_concept_id", "stop_reason", "refills", "quantity",
        "days_supply", "route_concept_id", "effective_drug_dose", "dose_unit_concept_ id",
        "lot_number", "provider_id", "visit_occurrence_id", "drug_source_value",
        "drug_source_concept_id", "route_source_value", "dose_unit_source_value"
    ],
    'device_exposure': [
        "device_exposure_id", "person_id", "device_concept_id", "device_exposure_start_date",
        "device_exposure_start_datetime", "device_exposure_end_date", "device_exposure_end_datetime",
        "device_type_concept_id", "unique_device_id", "quantity", "provider_id", "visit_occurrence_id",
        "device_source_value", "device_source_concept_id"
    ],
}


tableDependencies = {
    'person': [],
    'visit_occurrence': [
        "deliriumscore", "painscore", "cognition", "anxiety", "mobilization",
        "nutrition", "foreignobjects", "perioop", "admissionstatus", "alcoholscore",
        "depressionscore", "fallrisk", "frailtyScore", "minicog", "nicotinconsumptionscore",
        "polypharmaziescore", "precipitatingfactors", "predispositionfactors", "sozialesituationscore", "timedupandgoscore",
    ],
    'visit_detail': [],
    'death': [],
    'procedure_occurrence': ["cognition", "anxiety", "mobilization", "nutrition", "dysphagia", "mouthhygiene"], #"foreignobject", 
    'measurement': ["deliriumscore", "painscore", "anxiety", "minicog", "frailtyScore", "predispositionfactors"],
    'observation': ["precipitatingfactors", "predispositionfactors"],
    'condition_occurrence': [],
    'drug_exposure': [],
    'device_exposure': [],
}


@dataclass
class OMOPTablesAttributesHandler:
    @classmethod
    def getTableDependecies(cls, tableName):  #TODO: refazer os table dependecies -> quero um lista os keys que eu preciso para cada TABLE
        return tableDependencies.get(tableName)  ## TODO: Aqui as opcoes: deliriumScores painScores cognition anxiety mobilization nutrition foreignObjects perioOP
                                                 ## PreOP: admissionStatus alcoholScore depressionScore fallRisk frailtyScore miniCog nicotinconsumptionScore polypharmazieScore precipitatingFactors predispositionFactors sozialeSituationScore timedUpAndGoScore
    @classmethod
    def getTableSchema(cls, tableName):
        return tableSchemas.get(tableName)

    @classmethod
    def getTableColumns(cls, tableName):
        return list(tableSchemas.get(tableName).keys())

    @classmethod
    def getOMOPTableNames(cls):
        tableDependenciesForProcessing = copy.deepcopy(tableDependencies)
        tableDependenciesForProcessing.pop('person')
        return tableDependenciesForProcessing.keys()
