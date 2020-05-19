from __future__ import print_function
from rucio.client import Client
import pickle

client = Client(
    account="transfer_ops",
)

regionmap = {
    'T1_DE_KIT_Disk': 'A',
    'T1_ES_PIC_Disk': 'C',
    'T1_FR_CCIN2P3_Disk': 'A',
    'T1_IT_CNAF_Disk': 'C',
    'T1_RU_JINR_Disk': 'D',
    'T1_UK_RAL_Disk': 'A',
    'T1_US_FNAL_Disk': 'B',
    'T2_AT_Vienna': 'A',
    'T2_BE_IIHE': 'C',
    'T2_BE_UCL': 'C',
    'T2_BR_SPRACE': 'B',
    'T2_BR_UERJ': 'B',
    'T2_CH_CERN': 'A',
    'T2_CH_CSCS': 'A',
    'T2_CN_Beijing': 'D',
    'T2_DE_DESY': 'A',
    'T2_DE_RWTH': 'C',
    'T2_EE_Estonia': 'D',
    'T2_ES_CIEMAT': 'C',
    'T2_ES_IFCA': 'C',
    'T2_FI_HIP': 'D',
    'T2_FR_CCIN2P3': 'A',
    'T2_FR_GRIF_IRFU': 'A',
    'T2_FR_GRIF_LLR': 'A',
    'T2_FR_IPHC': 'A',
    'T2_GR_Ioannina': 'C',
    'T2_HU_Budapest': 'A',
    'T2_IN_TIFR': 'D',
    'T2_IT_Bari': 'A',
    'T2_IT_Legnaro': 'C',
    'T2_IT_Pisa': 'C',
    'T2_IT_Rome': 'A',
    'T2_KR_KISTI': 'D',
    'T2_KR_KNU': 'D',
    'T2_PK_NCP': 'D',
    'T2_PL_Swierk': 'D',
    'T2_PL_Warsaw': 'C',
    'T2_PT_NCG_Lisbon': 'C',
    'T2_RU_IHEP': 'D',
    'T2_RU_INR': 'D',
    'T2_RU_ITEP': 'D',
    'T2_RU_JINR': 'D',
    'T2_TR_METU': 'C',
    'T2_TW_NCHC': 'D',
    'T2_UA_KIPT': 'C',
    'T2_UK_London_Brunel': 'A',
    'T2_UK_London_IC': 'A',
    'T2_UK_SGrid_Bristol': 'C',
    'T2_UK_SGrid_RALPP': 'A',
    'T2_US_Caltech': 'B',
    'T2_US_Florida': 'B',
    'T2_US_MIT': 'B',
    'T2_US_Nebraska': 'B',
    'T2_US_Purdue': 'B',
    'T2_US_UCSD': 'B',
    'T2_US_Vanderbilt': 'B',
    'T2_US_Wisconsin': 'B',
}

for rse, region in regionmap.items():
    try:
        client.add_rse_attribute(rse, "region", region)
        client.add_rse_attribute(rse + "_Test", "region", region)
    except:
        print("Failed to set region for", rse)
        raise
