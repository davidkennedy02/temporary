#!/usr/bin/env python3
"""
Configuration Utility for CSV to HL7 Converter

This script allows you to easily customize the configuration settings
for the CSV to HL7 converter without editing JSON files directly.
"""

from config_manager import config
import sys
import os

def show_current_config():
    """Display the current configuration settings."""
    print("\n=== Current Configuration ===")
    print(f"Input Folder: {config.get_input_folder()}")
    print(f"Output Folder: {config.get_output_folder()}")
    print(f"Batch Size: {config.get_batch_size()}")
    print(f"Max Workers: {config.get_max_workers()}")
    print(f"Max Retries: {config.get_max_retries()}")
    print("\n=== HL7 Settings ===")
    print(f"Sending Application: {config.get_sending_application()}")
    print(f"Sending Facility: {config.get_sending_facility()}")
    print(f"Receiving Application: {config.get_receiving_application()}")
    print(f"Receiving Facility: {config.get_receiving_facility()}")
    print(f"Default Event Type: {config.get_default_event_type()}")
    print(f"HL7 Version: {config.get_hl7_version()}")
    print(f"Processing ID: {config.get_processing_id()}")
    print("\n=== Patient Settings ===")
    print(f"Assigning Authority: {config.get_assigning_authority()}")
    print("\n=== PV1 Settings ===")
    print(f"Patient Class: {config.get_pv1_patient_class()}")
    print(f"Patient Type: {config.get_pv1_patient_type()}")
    print(f"Visit Institution: {config.get_pv1_visit_institution()}")
    print(f"Attending Doctor ID: {config.get_pv1_attending_doctor_id()}")
    print(f"Attending Doctor Name: {config.get_pv1_attending_doctor_name()}")
    print(f"Referring Doctor Name: {config.get_pv1_referring_doctor_name()}")
    print("\n=== Logging ===")
    print(f"Log Directory: {config.get_log_directory()}")
    print(f"Log Level: {config.get_log_level()}")
    print()

def validate_config():
    """Validate the current configuration."""
    issues = config.validate_config()
    if issues:
        print("\n❌ Configuration Issues Found:")
        for issue in issues:
            print(f"  • {issue}")
        return False
    else:
        print("\n✅ Configuration is valid!")
        return True

def customize_directories():
    """Customize input and output directories."""
    print("\n=== Directory Configuration ===")
    
    current_input = config.get_input_folder()
    new_input = input(f"Input folder (current: {current_input}): ").strip()
    if new_input:
        config.set('directories.input_folder', new_input)
        print(f"✓ Input folder updated to: {new_input}")
    
    current_output = config.get_output_folder()
    new_output = input(f"Output folder (current: {current_output}): ").strip()
    if new_output:
        config.set('directories.output_folder', new_output)
        print(f"✓ Output folder updated to: {new_output}")

def customize_hl7_settings():
    """Customize HL7 message settings."""
    print("\n=== HL7 Message Configuration ===")
    
    current_sending_app = config.get_sending_application()
    new_sending_app = input(f"Sending Application (current: {current_sending_app}): ").strip()
    if new_sending_app:
        config.set('hl7_settings.sending_application', new_sending_app)
        print(f"✓ Sending Application updated to: {new_sending_app}")
    
    current_sending_facility = config.get_sending_facility()
    new_sending_facility = input(f"Sending Facility (current: {current_sending_facility}): ").strip()
    if new_sending_facility:
        config.set('hl7_settings.sending_facility', new_sending_facility)
        print(f"✓ Sending Facility updated to: {new_sending_facility}")
    
    current_receiving_app = config.get_receiving_application()
    new_receiving_app = input(f"Receiving Application (current: {current_receiving_app}): ").strip()
    if new_receiving_app:
        config.set('hl7_settings.receiving_application', new_receiving_app)
        print(f"✓ Receiving Application updated to: {new_receiving_app}")
    
    current_receiving_facility = config.get_receiving_facility()
    new_receiving_facility = input(f"Receiving Facility (current: {current_receiving_facility}): ").strip()
    if new_receiving_facility:
        config.set('hl7_settings.receiving_facility', new_receiving_facility)
        print(f"✓ Receiving Facility updated to: {new_receiving_facility}")
    
    current_event = config.get_default_event_type()
    print(f"\nDefault Event Type (current: {current_event})")
    print("Valid options: A01, A04, A08, A28, A31")
    new_event = input("New event type: ").strip().upper()
    if new_event and new_event in ['A01', 'A04', 'A08', 'A28', 'A31']:
        config.set('hl7_settings.default_event_type', new_event)
        print(f"✓ Default Event Type updated to: {new_event}")
    elif new_event:
        print("❌ Invalid event type. Must be one of: A01, A04, A08, A28, A31")

def customize_patient_settings():
    """Customize patient-related settings."""
    print("\n=== Patient Configuration ===")
    
    current_authority = config.get_assigning_authority()
    new_authority = input(f"Assigning Authority (current: {current_authority}): ").strip()
    if new_authority:
        config.set('patient_settings.assigning_authority', new_authority)
        print(f"✓ Assigning Authority updated to: {new_authority}")

def customize_pv1_settings():
    """Customize PV1 segment settings."""
    print("\n=== PV1 Segment Configuration ===")
    
    current_patient_class = config.get_pv1_patient_class()
    print(f"\nPatient Class (current: {current_patient_class})")
    print("Valid options: I=Inpatient, O=Outpatient, E=Emergency, P=Preadmit, R=Recurring, B=Obstetrics")
    new_patient_class = input("New patient class: ").strip().upper()
    if new_patient_class and new_patient_class in ['I', 'O', 'E', 'P', 'R', 'B']:
        config.set('pv1_settings.patient_class', new_patient_class)
        print(f"✓ Patient Class updated to: {new_patient_class}")
    elif new_patient_class:
        print("❌ Invalid patient class. Must be one of: I, O, E, P, R, B")
    
    current_institution = config.get_pv1_visit_institution()
    new_institution = input(f"Visit Institution (current: {current_institution}): ").strip()
    if new_institution:
        config.set('pv1_settings.visit_institution', new_institution)
        print(f"✓ Visit Institution updated to: {new_institution}")
    
    current_attending_id = config.get_pv1_attending_doctor_id()
    new_attending_id = input(f"Attending Doctor ID (current: {current_attending_id}): ").strip()
    if new_attending_id:
        config.set('pv1_settings.attending_doctor_id', new_attending_id)
        print(f"✓ Attending Doctor ID updated to: {new_attending_id}")
    
    current_attending_name = config.get_pv1_attending_doctor_name()
    new_attending_name = input(f"Attending Doctor Name (current: {current_attending_name}): ").strip()
    if new_attending_name:
        config.set('pv1_settings.attending_doctor_name', new_attending_name)
        print(f"✓ Attending Doctor Name updated to: {new_attending_name}")
    
    current_referring_name = config.get_pv1_referring_doctor_name()
    new_referring_name = input(f"Referring Doctor Name (current: {current_referring_name}): ").strip()
    if new_referring_name:
        config.set('pv1_settings.referring_doctor_name', new_referring_name)
        print(f"✓ Referring Doctor Name updated to: {new_referring_name}")
    
    current_referring_id = config.get_pv1_referring_doctor_id()
    new_referring_id = input(f"Referring Doctor ID (current: {current_referring_id}): ").strip()
    if new_referring_id:
        config.set('pv1_settings.referring_doctor_id', new_referring_id)
        print(f"✓ Referring Doctor ID updated to: {new_referring_id}")

def customize_processing():
    """Customize processing settings."""
    print("\n=== Processing Configuration ===")
    
    current_batch = config.get_batch_size()
    new_batch = input(f"Batch Size (current: {current_batch}): ").strip()
    if new_batch:
        try:
            batch_size = int(new_batch)
            if batch_size > 0:
                config.set('processing.batch_size', batch_size)
                print(f"✓ Batch Size updated to: {batch_size}")
            else:
                print("❌ Batch size must be positive")
        except ValueError:
            print("❌ Invalid batch size. Must be a number.")
    
    current_workers = config.get_max_workers()
    new_workers = input(f"Max Workers (current: {current_workers}, 0 for auto): ").strip()
    if new_workers:
        try:
            workers = int(new_workers)
            if workers >= 0:
                config.set('processing.max_workers', workers if workers > 0 else None)
                print(f"✓ Max Workers updated to: {workers if workers > 0 else 'auto'}")
            else:
                print("❌ Max workers must be non-negative")
        except ValueError:
            print("❌ Invalid worker count. Must be a number.")

def main():
    """Main configuration utility."""
    print("🏥 CSV to HL7 Converter - Configuration Utility")
    print("=" * 50)
    
    while True:
        print("\nOptions:")
        print("1. Show current configuration")
        print("2. Validate configuration")
        print("3. Customize directories")
        print("4. Customize HL7 settings")
        print("5. Customize patient settings")
        print("6. Customize PV1 settings")
        print("7. Customize processing settings")
        print("8. Save configuration")
        print("9. Reset to defaults")
        print("10. Exit")
        
        choice = input("\nSelect an option (1-10): ").strip()
        
        if choice == '1':
            show_current_config()
        elif choice == '2':
            validate_config()
        elif choice == '3':
            customize_directories()
        elif choice == '4':
            customize_hl7_settings()
        elif choice == '5':
            customize_patient_settings()
        elif choice == '6':
            customize_pv1_settings()
        elif choice == '7':
            customize_processing()
        elif choice == '8':
            config.save_config()
            print("\n✅ Configuration saved to config.json")
        elif choice == '9':
            confirm = input("\n⚠️  Reset to defaults? This will overwrite current settings (y/N): ")
            if confirm.lower() == 'y':
                config._config = config._get_default_config()
                config.save_config()
                print("✅ Configuration reset to defaults")
            else:
                print("Reset cancelled")
        elif choice == '10':
            print("\nGoodbye! 👋")
            break
        else:
            print("❌ Invalid option. Please choose 1-10.")

if __name__ == "__main__":
    main()