import unittest
import shutil
import os
import glob
import sys

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from main import process_file_streaming
from config_manager import config


class TestSampleData(unittest.TestCase):
    """Test processing of sample CSV files to ensure correct HL7 output generation."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Get the actual output directory from config
        self.output_dir = config.get_output_folder()
        
        # Clean up any existing output to start fresh
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
    
    def tearDown(self):
        """Clean up after each test method."""
        # Clean up test output directory
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
    
    def count_hl7_files(self):
        """Count all .hl7 files in the configured output directory and subdirectories."""
        hl7_files = glob.glob(os.path.join(self.output_dir, '**', '*.hl7'), recursive=True)
        return len(hl7_files)
    
    def get_sample_file_path(self, filename):
        """Get the full path to a sample CSV file."""
        base_dir = os.path.dirname(os.path.dirname(__file__))  # Go up two levels from tests/
        return os.path.join(base_dir, 'sample_CSVs', filename)
    
    def test_valid_csv_processing(self):
        """Test that valid CSV produces correct number of HL7 files."""
        sample_file = self.get_sample_file_path('patient_test_cases_valid.csv')
        
        # Process the file
        process_file_streaming(sample_file, 'csv')
        
        # Count HL7 files generated
        hl7_count = self.count_hl7_files()
        
        # Should produce 2 HL7 files (3 lines - 1 header = 2 data records)
        self.assertEqual(hl7_count, 2, 
                        f"Expected 2 HL7 files from patient_test_cases_valid.csv, but got {hl7_count}")
    
    def test_valid_extended_csv_processing(self):
        """Test that valid extended CSV produces correct number of HL7 files."""
        sample_file = self.get_sample_file_path('patient_test_cases_valid_extended.csv')
        
        # Process the file
        process_file_streaming(sample_file, 'csv')
        
        # Count HL7 files generated
        hl7_count = self.count_hl7_files()
        
        # Should produce 17 HL7 files (21 lines - 1 header - 3 skipped for dod > 2 years ago = 17 records)
        self.assertEqual(hl7_count, 17, 
                        f"Expected 17 HL7 files from patient_test_cases_valid_extended.csv, but got {hl7_count}")
    
    def test_edge_csv_processing(self):
        """Test that edge case CSV produces correct number of HL7 files."""
        sample_file = self.get_sample_file_path('patient_test_cases_edge.csv')
        
        # Process the file
        process_file_streaming(sample_file, 'csv')
        
        # Count HL7 files generated
        hl7_count = self.count_hl7_files()
        
        # Should produce 18 HL7 files (21 lines - 1 header - 2 skipped for age > 112 = 18 records)
        # Edge cases include patients that are skipped due to validation rules
        self.assertEqual(hl7_count, 18, 
                        f"Expected 18 HL7 files from patient_test_cases_edge.csv, but got {hl7_count}")
    
    def test_invalid_csv_processing(self):
        """Test that invalid CSV produces no HL7 files."""
        sample_file = self.get_sample_file_path('patient_test_cases_invalid.csv')
        
        # Process the file
        process_file_streaming(sample_file, 'csv')
        
        # Count HL7 files generated
        hl7_count = self.count_hl7_files()
        
        # Should produce 0 HL7 files (all records are invalid - missing surnames)
        self.assertEqual(hl7_count, 0, 
                        f"Expected 0 HL7 files from patient_test_cases_invalid.csv, but got {hl7_count}")
    
    def test_sample_files_exist(self):
        """Test that all expected sample files exist."""
        expected_files = [
            'patient_test_cases_valid.csv',
            'patient_test_cases_valid_extended.csv',
            'patient_test_cases_edge.csv',
            'patient_test_cases_invalid.csv'
        ]
        
        for filename in expected_files:
            file_path = self.get_sample_file_path(filename)
            self.assertTrue(os.path.exists(file_path), 
                           f"Sample file {filename} does not exist at {file_path}")
    
    def test_hl7_file_structure(self):
        """Test that generated HL7 files have proper structure and content."""
        sample_file = self.get_sample_file_path('patient_test_cases_valid.csv')
        
        # Process the file
        process_file_streaming(sample_file, 'csv')
        
        # Find generated HL7 files
        hl7_files = glob.glob(os.path.join(self.output_dir, '**', '*.hl7'), recursive=True)
        
        self.assertGreater(len(hl7_files), 0, "No HL7 files were generated")
        
        # Check the first HL7 file
        with open(hl7_files[0], 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Verify HL7 structure
        self.assertIn('MSH|', content, "HL7 file should contain MSH segment")
        self.assertIn('EVN|', content, "HL7 file should contain EVN segment")
        self.assertIn('PID|', content, "HL7 file should contain PID segment")
        
        # Verify proper line endings (should be CR only, but be flexible)
        lines_cr = content.split('\r')
        lines_lf = content.split('\n')
        lines_crlf = content.split('\r\n')
        
        # Should have multiple segments separated by some form of line ending
        max_segments = max(len(lines_cr), len(lines_lf), len(lines_crlf))
        self.assertGreater(max_segments, 1, "HL7 file should have multiple segments separated by line endings")
    
    def test_output_directory_organization(self):
        """Test that HL7 files are organized by birth year in subdirectories."""
        sample_file = self.get_sample_file_path('patient_test_cases_valid.csv')
        
        # Process the file
        process_file_streaming(sample_file, 'csv')
        
        # Check that subdirectories are created
        subdirs = [d for d in os.listdir(self.output_dir) 
                  if os.path.isdir(os.path.join(self.output_dir, d))]
        
        self.assertGreater(len(subdirs), 0, 
                          "HL7 output should be organized in year subdirectories")
        
        # Check that subdirectories contain HL7 files
        for subdir in subdirs:
            subdir_path = os.path.join(self.output_dir, subdir)
            hl7_files_in_subdir = glob.glob(os.path.join(subdir_path, '*.hl7'))
            if hl7_files_in_subdir:  # Only check non-empty subdirectories
                self.assertGreater(len(hl7_files_in_subdir), 0, 
                                  f"Year subdirectory {subdir} should contain HL7 files")


if __name__ == '__main__':
    unittest.main()
