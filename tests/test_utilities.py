import unittest
from datetime import datetime, timedelta
from segments.segment_utilities import create_obr_time
from hl7_utilities import create_control_id, create_adt_message
from segments import create_msh, create_pid, create_pv1, create_evn
from patientinfo import Patient
from hl7apy.core import Message

class TestUtilities(unittest.TestCase):

    def test_create_obr_time(self):
        obr_time = create_obr_time()
        obr_datetime = datetime.strptime(obr_time, "%Y%m%d%H%M")
        self.assertTrue(datetime.now() - timedelta(days=7) <= obr_datetime <= datetime.now())

    def test_create_control_id(self):
        control_id = create_control_id()
        self.assertEqual(len(control_id), 20)
        self.assertTrue(control_id.isdigit())

    def test_create_msh(self):
        hl7 = Message("ADT_A01", version="2.4")
        control_id = create_control_id()
        current_date = datetime.now()
        hl7 = create_msh.create_msh("ADT^A01", control_id, hl7, current_date)
        self.assertIsNotNone(hl7)
        self.assertEqual(hl7.msh.msh_10.to_er7(), control_id)

    def test_create_pid(self):
        patient_info = Patient(
            internal_patient_number="123456789012",
            assigning_authority="RX1",
            hospital_case_number="H123456",
            nhs_number="1234567890",
            nhs_verification_status="01",
            surname="Doe",
            forename="John",
            date_of_birth="19900101",
            sex="M",
            patient_title="Mr",
            address_line_1="123 Main St",
            address_line_2="Apt 4",
            address_line_3="",
            address_line_4="",
            address_line_5="",
            postcode="12345",
            death_indicator="N",
            date_of_death="",
            registered_gp_code="GP123",
            ethnic_code="A",
            home_phone="1234567890",
            work_phone="",
            mobile_phone="0987654321",
            registered_gp="Dr. Smith",
            registered_practice="Practice1"
        )
        hl7 = Message("ADT_A01", version="2.4")
        hl7 = create_pid.create_pid(patient_info, hl7)
        self.assertIsNotNone(hl7)
        self.assertEqual(hl7.pid.pid_5.pid_5_1.to_er7(), "Doe")

    def test_create_pv1(self):
        hl7 = Message("ADT_A01", version="2.4")
        hl7 = create_pv1.create_pv1(hl7)
        self.assertIsNotNone(hl7)
        self.assertEqual(hl7.pv1.pv1_1.to_er7(), "1")

    def test_create_evn(self):
        hl7 = Message("ADT_A01", version="2.4")
        hl7 = create_evn.create_evn(hl7, event_type="A01")
        self.assertIsNotNone(hl7)
        self.assertEqual(hl7.evn.evn_1.to_er7(), "A01")

    def test_create_adt_message(self):
        patient_info = Patient(
            internal_patient_number="123456789012",
            assigning_authority="RX1",
            hospital_case_number="H123456",
            nhs_number="1234567890",
            nhs_verification_status="01",
            surname="Doe",
            forename="John",
            date_of_birth="19900101",
            sex="M",
            patient_title="Mr",
            address_line_1="123 Main St",
            address_line_2="Apt 4",
            address_line_3="",
            address_line_4="",
            address_line_5="",
            postcode="12345",
            death_indicator="N",
            date_of_death="",
            registered_gp_code="GP123",
            ethnic_code="A",
            home_phone="1234567890",
            work_phone="",
            mobile_phone="0987654321",
            registered_gp="Dr. Smith",
            registered_practice="Practice1"
        )
        hl7_message = create_adt_message(patient_info, event_type="A01")
        self.assertIsNotNone(hl7_message)
        self.assertEqual(hl7_message.msh.msh_9.to_er7(), "ADT^A01")

    def test_create_adt_a28_message(self):
        patient_info = Patient(
            internal_patient_number="123456789012",
            assigning_authority="RX1",
            hospital_case_number="H123456",
            nhs_number="1234567890",
            nhs_verification_status="01",
            surname="Doe",
            forename="John",
            date_of_birth="19900101",
            sex="M",
            patient_title="Mr",
            address_line_1="123 Main St",
            address_line_2="Apt 4",
            address_line_3="",
            address_line_4="",
            address_line_5="",
            postcode="12345",
            death_indicator="N",
            date_of_death="",
            registered_gp_code="GP123",
            ethnic_code="A",
            home_phone="1234567890",
            work_phone="",
            mobile_phone="0987654321",
            registered_gp="Dr. Smith",
            registered_practice="Practice1"
        )
        hl7_message = create_adt_message(patient_info, event_type="A28")
        self.assertIsNotNone(hl7_message)
        self.assertEqual(hl7_message.msh.msh_9.to_er7(), "ADT^A28")
        # A28 messages should not include PV1 segment in the output
        self.assertNotIn('PV1', hl7_message.to_er7())

if __name__ == "__main__":
    unittest.main()
