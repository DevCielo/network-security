from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.config_entity import ModelTrainerConfig
from networksecurity.components.model_trainer import ModelTrainer
from networksecurity.entity.artifact_entity import DataIngestionArtifact
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.entity.config_entity import TrainingPipelineConfig, DataValidationConfig, DataTransformationConfig
from networksecurity.logging.logger import logging
import sys

if __name__ == "__main__":
    try:
        # Data ingestion
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
        logging.info("Initiate the data ingestion")
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        logging.info("Data Initiation Completed")
        print(data_ingestion_artifact)

        # Data validation
        data_validation_config= DataValidationConfig(training_pipeline_config)
        data_validation = DataValidation(data_ingestion_artifact, data_validation_config)
        logging.info("Initiate the data validation")
        data_validation_artifact = data_validation.initiate_data_validation()
        logging.info("Data Validation Completed")
        print(data_validation_artifact)

        # Data transformation
        data_transformation_config = DataTransformationConfig(training_pipeline_config)
        logging.info("Data transformation started")
        data_transformation = DataTransformation(data_validation_artifact, data_transformation_config)
        data_transformation_artifact = data_transformation.initiate_data_transformation()
        print(data_transformation_artifact)
        logging.info("Data transformation completed")

        # Model trainer
        logging.info("Model trainer started")
        model_trainer_config = ModelTrainerConfig(training_pipeline_config)
        model_trainer = ModelTrainer(model_trainer_config, data_transformation_artifact)
        model_trainer_artifact = model_trainer.initiate_model_trainer()
        logging.info("Model trainer completed")

    except Exception as e:
        raise NetworkSecurityException(e, sys)