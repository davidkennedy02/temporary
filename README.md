# CSV & PAS to HL7 Processing Script

## Overview
This script is a configurable, high-performance CSV and PAS to HL7v2 converter designed for healthcare data processing. It processes patient data files from a configurable input folder, validates and transforms the data into `Patient` objects (defined in `patientinfo.py`), and generates standardized HL7v2 ADT (Admission, Discharge, Transfer) messages. The system supports multiple event types (A01, A04, A08, A28, A31) and uses modular segment generators in the `segments` directory to construct compliant HL7v2 messages. Output files are saved to a configurable output directory with organized folder structures based on patient birth years.

**Key Features:**
- **Configurable**: All settings managed via JSON configuration
- **High Performance**: Multi-core parallel processing with batching
- **Robust**: Comprehensive error handling and data validation
- **Healthcare Compliant**: Generates valid HL7v2 messages with proper formatting
- **Scalable**: Handles large datasets with memory-efficient streaming

## Table of Contents

1. [Project Structure](#project-structure)
2. [Data Validation](#data-validation)
3. [HL7 Message Construction](#hl7-message-construction)
4. [File Naming Convention](#file-naming-convention)
5. [Configuration](#configuration)
   - [Configuration Options](#configuration-options)
   - [Configuration Methods](#configuration-methods)
   - [Example Configuration](#example-configuration)
6. [Script Execution](#script-execution)
   - [Prerequisites](#prerequisites)
   - [Quick Start](#quick-start)
   - [Detailed Steps](#detailed-steps)
   - [Performance Notes](#performance-notes)
7. [Logging](#logging)
8. [Log Searching and Analysis](#log-searching-and-analysis)
9. [Testing](#testing)
10. [Important Notes](#important-notes)
11. [Contact Information](#contact-information)

Please make sure to review the [important notes](#important-notes). 

If you have any questions or queries, please refer to the [contact information](#contact-information) below.



## Project Structure 
The directory layout should look as follows:
- CSVTOHL7 (root of directory)
    - logs
    - sample_CSVs
        - ``patient_test_cases_edge.csv``
        - ``patient_test_cases_invalid.csv``
        - ``patient_test_cases_valid_extended.csv``
        - ``patient_test_cases_valid.csv``
    - segments
        - ``create_evn.py``
        - ``create_msh.py``
        - ``create_pid.py``
        - ``create_pv1.py``
        - ``segment_utilities.py``
    - tests
        - ``test_utilities.py``
        - ``test_sample_data.py``
    - ``config.json`` (auto-generated configuration file)
    - ``config_manager.py`` (configuration management module)
    - ``configure.py`` (interactive configuration utility)
    - ``CONFIGURATION_GUIDE.md`` (detailed configuration documentation)
    - ``hl7_utilities.py``
    - ``LFchecker.py`` (line feed checker utility)
    - ``logger.py``
    - ``main.py``
    - ``patientinfo.py``
    - ``README.md``
    - ``requirements.txt``

**Note:** The input and output directory paths are configurable via `config.json` and no longer need to be hardcoded in the source files. The configuration system automatically handles path management and validation. 

## Data Validation
The system implements comprehensive data validation during `Patient` object creation:

- **Field Length Validation**: Ensures all fields comply with maximum permitted lengths
- **Data Format Validation**: Validates dates (YYYYMMDD format), phone numbers, and NHS numbers
- **Data Quality Checks**: Flags inconsistencies (e.g., death indicator vs. date of death)
- **Rule Validation**: Applies healthcare-specific rules (age limits, date relationships)
- **Error Logging**: All validation issues are logged with patient identifiers for data quality review
- **Graceful Degradation**: Invalid records are skipped with detailed logging rather than stopping processing

## HL7 Message Construction
HL7v2 message construction follows healthcare industry standards:

**Supported Message Types:**
- **ADT^A01**: Admit/Visit Notification
- **ADT^A04**: Register a Patient  
- **ADT^A08**: Update Patient Information
- **ADT^A28**: Add Person Information (default)
- **ADT^A31**: Update Person Information

**Message Segments:**
- **MSH**: Message Header (configurable sending/receiving systems)
- **EVN**: Event Type (with timestamps)
- **PID**: Patient Identification (demographics, identifiers, addresses)
- **PV1**: Patient Visit Information (for A01 events)

**Message Generation Process:**
1. Validated `Patient` object created from input data
2. HL7 message template instantiated using `hl7apy` library
3. Modular segment generators populate each segment
4. Message validated and serialized with proper HL7 formatting
5. Files saved with unique timestamps and organized by birth year

## File Naming Convention
**Output Structure:**
```
output_hl7/
├── 1980/
│   ├── 20241203141523.00000001.hl7
│   └── 20241203141523.00000002.hl7
├── 1985/
│   └── 20241203141524.00000003.hl7
└── unknown/
    └── 20241203141525.00000004.hl7
```

**Naming Convention:**
- **Format**: `YYYYMMDDHHMMSS.NNNNNNNN.hl7`
- **YYYYMMDDHHMMSS**: Timestamp when message was created
- **NNNNNNNN**: 8-digit sequence number for uniqueness
- **Directory**: Organized by patient birth year (or 'unknown' if invalid/missing DOB)
- **Line Endings**: Proper HL7 line endings (CR only, ASCII 13) for healthcare system compatibility

## Configuration

The script is now fully configurable via a JSON configuration file (`config.json`). On first run, a default configuration file will be created automatically.

### Configuration Options

- **Directories**: 
  - `input_folder`: Source directory for CSV/PAS files
  - `output_folder`: Destination directory for HL7 files

- **HL7 Settings**:
  - `sending_application`: MSH-3 Sending Application
  - `sending_facility`: MSH-4 Sending Facility
  - `receiving_application`: MSH-5 Receiving Application
  - `receiving_facility`: MSH-6 Receiving Facility
  - `default_event_type`: Default ADT event type (A01, A04, A08, A28, A31)
  - `hl7_version`: HL7 standard version (default: 2.4)
  - `processing_id`: MSH-11 Processing ID (T=Test, P=Production)
  - `accept_acknowledgment_type`: MSH-15 Accept Acknowledgment Type
  - `application_acknowledgment_type`: MSH-16 Application Acknowledgment Type

- **Patient Settings**:
  - `assigning_authority`: PID-3.4 Assigning Authority for patient identifiers

- **PV1 Settings** (Patient Visit Information):
  - `patient_class`: PV1-2 Patient Class (I=Inpatient, O=Outpatient, E=Emergency, etc.)
  - `patient_type`: PV1-2 Patient Type (for compatibility)
  - `visit_institution`: PV1-3 Assigned Patient Location/Institution
  - `attending_doctor_id`: PV1-7 Attending Doctor ID
  - `attending_doctor_name`: PV1-8 Attending Doctor Name
  - `attending_doctor_type`: PV1-8 Attending Doctor Type/Degree
  - `referring_doctor_name`: PV1-9 Referring Doctor Name
  - `referring_doctor_id`: PV1-9 Referring Doctor ID

- **Processing**:
  - `batch_size`: Records per processing batch (default: 1000)
  - `max_workers`: Parallel processing workers (null = auto-detect)
  - `max_retries`: Retry attempts for failed batches

- **Logging**:
  - `log_directory`: Directory for log files (default: 'logs')
  - `log_level`: Logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL)

### Configuration Methods

1. **Automatic Setup**: Run the script and a default `config.json` will be created
   ```bash
   python main.py  # Creates config.json with dummy defaults
   ```

2. **Interactive Configuration**: Use the configuration utility for guided setup:
   ```bash
   python configure.py  # User-friendly configuration wizard
   ```

3. **Manual Editing**: Edit `config.json` directly with any text editor
   ```bash
   nano config.json  # Or use your preferred editor
   ```

4. **Validation**: Check configuration validity:
   ```bash
   python -c "from config_manager import config; issues = config.validate_config(); print('✅ Valid' if not issues else '❌ Issues: ' + str(issues))"
   ```

### Example Configuration

```json
{
  "directories": {
    "input_folder": "input",
    "output_folder": "output_hl7"
  },
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
  },
  "patient_settings": {
    "assigning_authority": "RX1"
  },
  "pv1_settings": {
    "patient_class": "O",
    "patient_type": "O",
    "visit_institution": "MAIN_HOSPITAL",
    "attending_doctor_id": "ACON",
    "attending_doctor_name": "ANAESTHETICS CONS",
    "attending_doctor_type": "L",
    "referring_doctor_name": "ANAESTHETICS CONS",
    "referring_doctor_id": "AUSHICPR"
  },
  "processing": {
    "batch_size": 1000,
    "max_workers": null,
    "max_retries": 3
  },
  "logging": {
    "log_directory": "logs",
    "log_level": "INFO"
  }
}
```

## Script Execution

### Prerequisites
1. **Python 3.7+** (tested with Python 3.13)
2. **Install Dependencies**: 
   ```bash
   python -m pip install -r requirements.txt
   ```

### Quick Start
```bash
# 1. Configure settings (optional - defaults will be created)
python configure.py

# 2. Add your CSV/PAS files to input folder
cp your_patient_data.csv input/

# 3. Run the converter
python main.py
```

### Detailed Steps

1. **Configure Settings** (recommended for first-time setup):
   ```bash
   python configure.py  # Interactive configuration wizard
   ```

2. **Prepare Input Files**: 
   - Place CSV and PAS files in the configured input folder (default: `input/`)
   - Supported formats: `.csv` (comma-separated) and `.txt` (PAS format)
   - Files should contain 25 columns of patient data as per the expected schema

3. **Run the Script**:
   ```bash
   python main.py
   ```

4. **Monitor Progress**:
   - Configuration validation occurs on startup
   - Current settings are logged for transparency
   - Processing progress is logged in real-time
   - Check `logs/` directory for detailed processing logs

5. **Review Output**:
   - HL7 messages saved to configured output folder (default: `output_hl7/`)
   - Files organized by patient birth year for easier management
   - Each message is a separate `.hl7` file with unique naming

### Performance Notes
- **Parallel Processing**: Automatically uses available CPU cores minus one
- **Memory Efficient**: Streams large files without loading entirely into memory
- **Batch Processing**: Processes records in configurable batches (default: 1000)
- **Error Recovery**: Failed batches are automatically retried up to 3 times

## Logging
Comprehensive logging system designed for healthcare data quality and operational monitoring:

**Log Features:**
- **Daily Rotation**: Log files rotate daily with timestamp suffixes (app-YYYYMMDD.log)
- **Multiple Levels**: DEBUG, INFO, WARNING, ERROR, CRITICAL with configurable verbosity
- **Patient Tracking**: All issues reference patient internal numbers for data quality investigation
- **Batch Tracking**: Processing statistics and performance metrics per batch
- **Error Context**: Full stack traces and context for debugging
- **Console Output**: Real-time feedback during processing

**Log Locations:**
- **Default Directory**: `logs/` (configurable)
- **Retention**: 7 days of historical logs maintained automatically
- **Format**: `YYYY-MM-DD HH:MM:SS - LEVEL - MESSAGE`

**Data Quality Alerts:**
- Invalid field lengths and formats
- Missing required data (surname, DOB)
- Date inconsistencies (death before birth)
- NHS number validation failures
- Hospital case number issues
- Encoding and parsing errors 

## Log Searching and Analysis

The comprehensive logging system makes it easy to search for specific issues, patients, or events using standard command-line tools like `grep`. Here are practical examples for common scenarios:

### Search by Log Level

**Find all errors:**
```bash
grep "ERROR" logs/app-*.log
```

**Find all warnings:**
```bash
grep "WARNING" logs/app-*.log
```

**Find critical issues:**
```bash
grep "CRITICAL" logs/app-*.log
```

**Find all errors and warnings:**
```bash
grep -E "(ERROR|WARNING)" logs/app-*.log
```

### Search by Patient

**Find all log entries for a specific patient:**
```bash
grep "TEST12345678" logs/app-*.log
```

**Find patients with missing surnames:**
```bash
grep "missing required surname" logs/app-*.log
```

**Find patients with data quality issues:**
```bash
grep -E "notify Data Quality team" logs/app-*.log
```

### Search by Processing Activity

**Find batch processing summaries:**
```bash
grep "Batch.*completed" logs/app-*.log
```

**Find file save operations:**
```bash
grep "Successfully saved HL7 message" logs/app-*.log
```

**Find processing failures:**
```bash
grep "Failed to" logs/app-*.log
```

**Find retry attempts:**
```bash
grep "Retrying.*failed batches" logs/app-*.log
```

### Search by Data Quality Issues

**Find NHS number validation issues:**
```bash
grep "NHS number.*over 10 chars\|NHS number.*non-numeric" logs/app-*.log
```

**Find hospital case number issues:**
```bash
grep "Hospital number.*over 25 chars" logs/app-*.log
```

**Find date format issues:**
```bash
grep "Invalid date" logs/app-*.log
```

**Find age-related exclusions:**
```bash
grep -E "age > 112|dod > 2 years ago" logs/app-*.log
```

### Advanced Search Examples

**Find today's processing activity:**
```bash
grep "$(date '+%Y-%m-%d')" logs/app-*.log
```

**Count errors by type:**
```bash
grep "ERROR" logs/app-*.log | cut -d'-' -f4- | sort | uniq -c | sort -nr
```

**Find patients processed in the last hour:**
```bash
grep "Patient initialized" logs/app-$(date '+%Y%m%d').log | tail -n 100
```

**Search across date range (last 3 days):**
```bash
for i in {0..2}; do
  date_str=$(date -d "$i days ago" '+%Y%m%d' 2>/dev/null || date -v-${i}d '+%Y%m%d')
  [ -f "logs/app-${date_str}.log" ] && grep "ERROR" "logs/app-${date_str}.log"
done
```

**Export search results to file:**
```bash
grep -E "(ERROR|WARNING)" logs/app-*.log > error_summary.txt
```

### Useful grep Options

- `-i`: Case-insensitive search
- `-n`: Show line numbers
- `-C 3`: Show 3 lines of context before and after matches
- `-A 5`: Show 5 lines after each match
- `-B 5`: Show 5 lines before each match
- `-c`: Count matching lines
- `-v`: Invert match (show non-matching lines)
- `-E`: Extended regex support
- `--color=always`: Highlight matches in color

**Example with context:**
```bash
grep -C 3 --color=always "TEST12345678" logs/app-*.log
```

These search patterns help quickly identify processing issues, track specific patients through the system, and analyze data quality problems for investigation.

## Testing
**Test Resources:**
- **Sample Data**: Test cases stored in `sample_CSVs/` folder:
  - `patient_test_cases_valid.csv`: Clean, valid patient records
  - `patient_test_cases_valid_extended.csv`: Extended valid test cases
  - `patient_test_cases_edge.csv`: Edge cases and boundary conditions
  - `patient_test_cases_invalid.csv`: Invalid data for error handling tests

- **Test Utilities**: Comprehensive testing framework in `tests/` folder
  - `test_utilities.py`: Unit tests for HL7 utilities and data validation
  - `test_sample_data.py`: Integration tests using sample CSV files
  - HL7 message structure validation
  - End-to-end processing pipeline tests

**Quality Assurance Features:**
- **Comprehensive Error Handling**: Try-catch blocks throughout all processing
- **Data Validation**: Multiple layers of validation before HL7 generation
- **Graceful Degradation**: System continues processing when individual records fail
- **Batch Retry Logic**: Automatic retry of failed processing batches
- **Configuration Validation**: Startup validation prevents runtime configuration errors

**Recommended Testing:**
```bash
# Run utility unit tests
python -m unittest tests.test_utilities -v

# Run sample data integration tests
python -m unittest tests.test_sample_data -v

# Run all tests
python -m unittest discover tests -v

# Test with sample data manually
cp sample_CSVs/patient_test_cases_valid.csv input/
python main.py

# Verify HL7 output format
ls -la output_hl7/*/
head output_hl7/*/*.hl7
``` 

## Important Notes

**⚠️ Healthcare Compliance:**
- This system generates HL7v2 messages for healthcare data exchange
- Ensure compliance with local healthcare data regulations (HIPAA, GDPR, etc.)
- Test thoroughly in non-production environments before clinical use
- Validate message acceptance with receiving systems

**🔧 Configuration:**
- Review and customize `config.json` for your environment
- Validate HL7 field mappings match your requirements
- Test event types (A01, A28, etc.) with receiving systems
- Consider backup and disaster recovery for processing environments

**📊 Data Quality:**
- Monitor logs for data quality issues requiring investigation
- Invalid records are skipped but logged for manual review
- Large age discrepancies (>112 years) are automatically excluded
- Death records >2 years old are filtered out by default

**🚀 Performance:**
- System designed for high-volume processing with parallel execution
- Memory usage optimized for large file processing
- Processing speed scales with available CPU cores
- Consider system resources when setting batch sizes and worker counts

## Contact Information
For any questions regarding this script, please contact David Kennedy or Nicholas Campbell by email at [david.kennedy@cirdan.com](mailto:david.kennedy@cirdan.com) or [nicholas.campbell@cirdan.com](mailto:nicholas.campbell@cirdan.com) on weekdays from 9am to 5pm GMT.

