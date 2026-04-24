from run_check import check_files
from file_ops import copy_file_locally, validate_checksum
import logging
import json

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(funcName)s] - %(message)s'
    )
    
    test_local_dir = "./tmp"
    
    # Test copy_file_locally with a valid PFN and an invalid PFN, and print results
    
    print("\n1.1. Testing copy_file_locally - valid PFN.")
    test_pfn1 = "davs://hip-cms-se.csc.fi:2880/store/user/nbinnorj/RAWToPFNANO_v0p1/CRABOUTPUT/DYto2Mu-4Jets_Bin-MLL-50_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAOD_HCalPFCuts_eta2829depth1_thresh4p0_RAWToPFNANO_v0p1/260224_232450/0001/NANOAODSIM_1721.root"
    local_file1 = copy_file_locally(test_pfn1, test_local_dir)
    print(f"Test 1.1. completed. local_file1 = '{local_file1}'")
    
    print("\n1.2. Testing copy_file_locally - invalid PFN.")
    test_pfn2 = "davs://hip-cms-se.csc.fi:2880/store/user/nbinnorj/RAWToPFNANO_v0p1/CRABOUTPUT/DYto2Mu-4Jets_Bin-MLL-50_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAOD_HCalPFCuts_eta2829depth1_thresh4p0_RAWToPFNANO_v0p1/260224_232450/0001/NANOAODSIM_1721_invalid.root"
    local_file2 = copy_file_locally(test_pfn2, test_local_dir)
    print(f"Test 1.2. completed. local_file2 = '{local_file2}'")

    print("\n2.1. Testing validate_checksum - valid checksum.")
    true_adler = "d34b712d"
    is_valid, status = validate_checksum(local_file1, true_adler)
    print(f"Test 2.1 completed. Validation result: {is_valid}, Status: {status}")
    
    print("\n2.2. Testing validate_checksum - invalid checksum.")
    false_adler = "00000000"
    is_valid, status = validate_checksum(local_file1, false_adler)
    print(f"Test 2.2 completed. Validation result: {is_valid}, Status: {status}")
    
    
    # Test check_files with a couple of LFNs (one with scope, one without) and print results
    test_lfns = [
        "cms:/store/mc/RunIISummer16NanoAODv3/CITo2E_M1300_CUETP8M1_Lam16TeVConLR_13TeV-pythia8/NANOAODSIM/PUMoriond17_94X_mcRun2_asymptotic_v3-v2/110000/1C867D77-39A3-E911-ABC8-0242AC1C0503.root",
        "/store/mc/RunIISummer16NanoAODv5/CITo2Mu_M800_CUETP8M1_Lam16TeVDesLL_13TeV_Pythia8_Corrected-v3/NANOAODSIM/PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1/30000/8BB32229-E898-B445-B871-4FC2840B46D5.root"
    ]

    print("\nTesting check_files - no rse_expression.")
    results = check_files(test_lfns, workdir="./tmp")
    print(json.dumps(results))

    print("\nTesting check_files - with rse_expression.")
    results = check_files(test_lfns, workdir="./tmp", rse_expression="rse_type=DISK")
    print(json.dumps(results))