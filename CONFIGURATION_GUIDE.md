# Configuration System Guide

## Overview

The CSV to HL7 Converter uses a comprehensive JSON-based configuration system that allows you to customize all aspects of HL7 message generation without modifying source code. This guide provides detailed information about all configuration options and their usage.

## Configuration File Structure

The configuration is stored in `config.json` with the following main sections:

### 1. Directories
Controls input and output folder locations:
```json
"directories": {
  "input_folder": "input",
  "output_folder": "output_hl7"
}
```

### 2. HL7 Settings
Configures HL7 message header (MSH segment) fields:
```json
"hl7_settings": {
  "sending_application": "CSV2HL7_Converter",
  "sending_facility": "Data_Processing_Center",
  "receiving_application": "Hospital_Information_System",
  "receiving_facility": "Main_Hospital",
  "default_event_type": "A28",
  "hl7_version": "2.4",
  "processing_id": "T",
  "accept_acknowledgment_type": "AL",
  "application_acknowledgment_type": "NE"
}
```

**Field Descriptions:**
- `sending_application`: MSH-3 - Identifies the sending application
- `sending_facility`: MSH-4 - Identifies the sending facility
- `receiving_application`: MSH-5 - Identifies the receiving application
- `receiving_facility`: MSH-6 - Identifies the receiving facility
- `default_event_type`: EVN-1 - Default ADT event type (A01, A04, A08, A28, A31)
- `hl7_version`: MSH-12 - HL7 version (typically "2.4")
- `processing_id`: MSH-11 - Processing ID (T=Test, P=Production, D=Debug)
- `accept_acknowledgment_type`: MSH-15 - Accept acknowledgment type
- `application_acknowledgment_type`: MSH-16 - Application acknowledgment type

### 3. Patient Settings
Configures patient identification fields:
```json
"patient_settings": {
  "assigning_authority": "RX1"
}
```

**Field Descriptions:**
- `assigning_authority`: PID-3.4 - Authority that assigned the patient identifier

### 4. PV1 Settings
Configures Patient Visit (PV1) segment fields:
```json
"pv1_settings": {
  "patient_class": "O",
  "patient_type": "O",
  "visit_institution": "MAIN_HOSPITAL",
  "attending_doctor_id": "ACON",
  "attending_doctor_name": "ANAESTHETICS CONS",
  "attending_doctor_type": "L",
  "referring_doctor_name": "ANAESTHETICS CONS",
  "referring_doctor_id": "AUSHICPR"
}
```

**Field Descriptions:**
- `patient_class`: PV1-2 - Patient class (I=Inpatient, O=Outpatient, E=Emergency, P=Preadmit, R=Recurring, B=Obstetrics)
- `patient_type`: Legacy compatibility field
- `visit_institution`: PV1-3 - Assigned patient location/institution name
- `attending_doctor_id`: PV1-7 - Attending physician identifier
- `attending_doctor_name`: PV1-8 - Attending physician name
- `attending_doctor_type`: PV1-8 - Attending physician degree/type (L=License, M=MD, etc.)
- `referring_doctor_name`: PV1-9 - Referring physician name  
- `referring_doctor_id`: PV1-9 - Referring physician identifier

### 5. Processing Settings
Controls batch processing and performance:
```json
"processing": {
  "batch_size": 1000,
  "max_workers": null,
  "max_retries": 3
}
```

**Field Descriptions:**
- `batch_size`: Number of records processed in each batch
- `max_workers`: Maximum parallel worker processes (null = auto-detect)
- `max_retries`: Number of retry attempts for failed batches

### 6. Logging Settings
Controls logging behavior:
```json
"logging": {
  "log_directory": "logs",
  "log_level": "INFO"
}
```

**Field Descriptions:**
- `log_directory`: Directory for log files
- `log_level`: Logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Configuration Methods

### Method 1: Interactive Configuration
Use the built-in configuration utility for guided setup:
```bash
python configure.py
```

This provides a menu-driven interface to:
- View current configuration
- Validate configuration
- Customize each section interactively
- Save changes
- Reset to defaults

### Method 2: Manual Editing
Edit `config.json` directly with any text editor:
```bash
vim config.json
# or
nano config.json
# or
code config.json
```

## Environment-Specific Configurations

### Development Environment
```json
{
  "directories": {
    "input_folder": "dev_input",
    "output_folder": "dev_output"
  },
  "hl7_settings": {
    "sending_application": "DEV_CSV2HL7",
    "processing_id": "D"
  },
  "processing": {
    "batch_size": 100,
    "max_workers": 2
  },
  "logging": {
    "log_level": "DEBUG"
  }
}
```

### Production Environment
```json
{
  "directories": {
    "input_folder": "/data/hl7_input",
    "output_folder": "/data/hl7_output"
  },
  "hl7_settings": {
    "sending_application": "PROD_CSV2HL7_v1.0",
    "sending_facility": "Regional_Data_Center",
    "receiving_application": "Epic_Hospital_System",
    "receiving_facility": "City_General_Hospital",
    "processing_id": "P"
  },
  "processing": {
    "batch_size": 5000,
    "max_workers": null
  },
  "logging": {
    "log_level": "INFO"
  }
}
```

## Validation and Testing

### Configuration Validation
Always validate your configuration before processing:
```bash
python -c "from config_manager import config; issues = config.validate_config(); print('✅ Valid' if not issues else '❌ Issues: ' + str(issues))"
```

### Testing Configuration Changes
1. **Backup existing configuration**:
   ```bash
   cp config.json config.json.backup
   ```

2. **Test with sample data**:
   ```bash
   cp sample_CSVs/patient_test_cases_valid.csv input/
   python main.py
   ```

3. **Verify HL7 output**:
   ```bash
   ls -la output_hl7/*/
   head output_hl7/*/*.hl7
   ```

## Troubleshooting

### Common Configuration Issues

1. **Invalid Event Type**:
   ```
   Error: Configuration 'hl7_settings.default_event_type' must be one of ['A01', 'A04', 'A08', 'A28', 'A31']
   ```
   Solution: Use a valid ADT event type.

2. **Missing Required Fields**:
   ```
   Error: Configuration 'hl7_settings.sending_application' is missing or empty
   ```
   Solution: Provide all required configuration values.

3. **Invalid Batch Size**:
   ```
   Error: Configuration 'processing.batch_size' must be a positive integer
   ```
   Solution: Set batch_size to a positive number.

### Configuration Recovery
If configuration becomes corrupted:
```bash
# Reset to defaults
python configure.py
# Choose option 9 (Reset to defaults)

# Or manually delete and regenerate
rm config.json
python main.py  # Will create default config
```

## API Reference

### ConfigManager Methods

#### Getter Methods
- `get_input_folder()` → str
- `get_output_folder()` → str
- `get_sending_application()` → str
- `get_sending_facility()` → str
- `get_receiving_application()` → str
- `get_receiving_facility()` → str
- `get_default_event_type()` → str
- `get_hl7_version()` → str
- `get_processing_id()` → str
- `get_assigning_authority()` → str
- `get_pv1_patient_class()` → str
- `get_pv1_visit_institution()` → str
- `get_pv1_attending_doctor_name()` → str
- `get_batch_size()` → int
- `get_max_workers()` → int
- `get_max_retries()` → int
- `get_log_directory()` → str
- `get_log_level()` → str

#### Utility Methods
- `validate_config()` → List[str]: Returns list of validation issues
- `save_config(path)`: Save configuration to file
- `load_config(path)`: Load configuration from file
- `get(key_path, default)`: Get configuration value using dot notation
- `set(key_path, value)`: Set configuration value using dot notation