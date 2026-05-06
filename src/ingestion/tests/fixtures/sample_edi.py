"""
EDI X12 837P string constants for unit tests.
Use these instead of reading from sample_data/ in every test to keep tests fast.
"""

# Standard mock claim — all fields present, prior auth present
MOCK_EDI_STANDARD = (
    "ISA*00*          *00*          *30*PAYER001       *30*PROVIDER001    *260412*1200*^*00501*000000001*0*P*:~"
    "GS*HC*PAYER001*PROVIDER001*20260412*1200*1*X*005010X222A1~"
    "ST*837*0001~"
    "BHT*0019*00*0001*20260412*1200*CH~"
    "NM1*41*2*MCK PAYER INC*****46*MCKPYR001~"
    "NM1*40*2*MCK HEALTH PLAN INC*****46*MCKPYR001~"
    "HL*1**20*1~"
    "NM1*85*2*MCK BILLING GRP LLC*****XX*9990000001~"
    "HL*2*1*22*0~"
    "NM1*IL*1*MCKLSTNM001*MCKFRSTNM001****MI*MCKMEMBR0001~"
    "CLM*CLM-MCK-20260101-001*350.00***11:B:1*Y*A*Y*I~"
    "DTP*472*D8*20260412~"
    "HI*ABK:Z00.00~"
    "NM1*82*1*MCKMDLSTNM*MCKMDFRSNM***MD*XX*9990000099~"
    "REF*D9*AUTHMCK0001~"
    "LX*1~"
    "SV1*HC:99213:25**350.00*UN*1***1~"
    "SE*17*0001~"
    "GE*1*1~"
    "IEA*1*000000001~"
)

# No prior auth — higher denial risk
MOCK_EDI_NO_PRIOR_AUTH = (
    "ISA*00*          *00*          *30*PAYER001       *30*PROVIDER001    *260412*1200*^*00501*000000002*0*P*:~"
    "GS*HC*PAYER001*PROVIDER001*20260412*1200*2*X*005010X222A1~"
    "ST*837*0002~"
    "BHT*0019*00*0002*20260412*1200*CH~"
    "NM1*41*2*MCK PAYER INC*****46*MCKPYR001~"
    "NM1*40*2*MCK HEALTH PLAN INC*****46*MCKPYR001~"
    "HL*1**20*1~"
    "NM1*85*2*MCK BILLING GRP LLC*****XX*9990000001~"
    "HL*2*1*22*0~"
    "NM1*IL*1*MCKLSTNM001*MCKFRSTNM001****MI*MCKMEMBR0001~"
    "CLM*CLM-MCK-20260101-002*350.00***11:B:1*Y*A*Y*I~"
    "DTP*472*D8*20260412~"
    "HI*ABK:Z00.00~"
    "LX*1~"
    "SV1*HC:99213*350.00*UN*1***1~"
    "SE*15*0002~"
    "GE*1*2~"
    "IEA*1*000000002~"
)

# High billed amount — triggers billed_amount risk factor
MOCK_EDI_HIGH_BILLED = (
    "ISA*00*          *00*          *30*PAYER001       *30*PROVIDER001    *260412*1200*^*00501*000000003*0*P*:~"
    "GS*HC*PAYER001*PROVIDER001*20260412*1200*3*X*005010X222A1~"
    "ST*837*0003~"
    "BHT*0019*00*0003*20260412*1200*CH~"
    "NM1*41*2*MCK PAYER INC*****46*MCKPYR001~"
    "NM1*40*2*MCK HEALTH PLAN INC*****46*MCKPYR001~"
    "HL*1**20*1~"
    "NM1*85*2*MCK BILLING GRP LLC*****XX*9990000001~"
    "HL*2*1*22*0~"
    "NM1*IL*1*MCKLSTNM001*MCKFRSTNM001****MI*MCKMEMBR0001~"
    "CLM*CLM-MCK-20260101-003*1500.00***11:B:1*Y*A*Y*I~"
    "DTP*472*D8*20260412~"
    "HI*ABK:Z00.00~"
    "LX*1~"
    "SV1*HC:99291*1500.00*UN*1***1~"
    "SE*15*0003~"
    "GE*1*3~"
    "IEA*1*000000003~"
)

# All expected parsed values for MOCK_EDI_STANDARD
EXPECTED_PARSED_STANDARD = {
    "transaction_id": "CLM-MCK-20260101-001",
    "billing_npi": "9990000001",
    "patient_member_id": "MCKMEMBR0001",
    "date_of_service": "2026-04-12",
    "diagnosis_codes": ["Z00.00"],
    "procedure_codes": ["99213"],
    "billed_amount": 350.0,
    "prior_auth_number": "AUTHMCK0001",
    "rendering_npi": "9990000099",
}
