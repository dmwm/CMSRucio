"""
RESSURECT BLOCKS
INPUT: blocks.txt

/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/RunIIFall18wmLHEGS-VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/GEN-SIM#071adca4-e33d-4137-b2f3-a88ceec57df2
/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/RunIIFall18wmLHEGS-VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/GEN-SIM#71034aed-f14b-46e6-9f96-a77aa723f344
/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/RunIIFall18wmLHEGS-VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/GEN-SIM#186e60f2-ad4e-4643-b6b8-3af894f0f37b



"""
from rucio.client import Client
client = Client()

block_dids = [] # 313 blocks
with open("./blocks.txt", "r") as blocks:
    for block in blocks:
        block_dids.append({'scope': 'cms', 'name':block.strip(), 'type': 'DATASET'})

client.resurrect(dids=block_dids)


"""
RESSURECT FILES (in chuncks of 500)
INPUT: files.txt

/store/mc/RunIIFall18wmLHEGS/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/GEN-SIM/VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/90001/17F92391-80F9-9947-A700-6E10C2EFC6BF.root,253165146,bd624b5c
/store/mc/RunIIFall18wmLHEGS/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/GEN-SIM/VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/30000/6A5C5723-9B10-E346-B3E3-92DDEE46793C.root,2052471395,24ca1b15
/store/mc/RunIIFall18wmLHEGS/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/GEN-SIM/VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/30000/BB2A8199-56F7-6943-9A79-E2F0DCDA47FD.root,495981052,8b681234

"""
from rucio.client import Client
client = Client()


def chunk_list(lst, chunk_size):
    # Loop from 0 to the length of the list, in steps of chunk_size
    for i in range(0, len(lst), chunk_size):
        # Yield a slice of the list from i to i + chunk_size
        yield lst[i:i + chunk_size]


file_dids = [] # 4602 files
with open("./files.txt", "r") as files:
    for file in files:
        file_name = file.strip().split(',')[0]
        file_dids.append({'scope': 'cms', 'name':file_name, 'type': 'FILE'})

# Slice list to ressurect in batches of 500
chunks = list(chunk_list(file_dids, 500))

for chunk in chunks:
    client.resurrect(dids=chunk)




"""
ATTACH BLOCKS TO DATASETS and FILES TO BLOCKS
Running this twice for the two csv files

INPUT: container_datasets.csv and datasets_files.csv 

"/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/RunIIFall18wmLHEGS-VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/GEN-SIM","/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/RunIIFall18wmLHEGS-VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/GEN-SIM#071adca4-e33d-4137-b2f3-a88ceec57df2"
"/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/RunIIFall18wmLHEGS-VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/GEN-SIM","/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/RunIIFall18wmLHEGS-VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/GEN-SIM#71034aed-f14b-46e6-9f96-a77aa723f344"

"/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/RunIIFall18wmLHEGS-VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/GEN-SIM#02a795af-1079-47be-86cc-28c945f737bd","/store/mc/RunIIFall18wmLHEGS/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/GEN-SIM/VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/90001/5537F144-780F-CC4F-9F8E-6570A347FF09.root"
"/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/RunIIFall18wmLHEGS-VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/GEN-SIM#04232855-6131-4790-be27-93b48cbc5b1d","/store/mc/RunIIFall18wmLHEGS/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/GEN-SIM/VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/80000/E2A722AC-A875-E543-96F0-9DFFF7508504.root"

"""
from rucio.client import Client
import csv

client = Client()

contents = {}

csv_file_path=''

with open(csv_file_path, mode='r') as file:
    reader = csv.reader(file)
    for row in reader:
        did = row[0].strip('"')
        child = row[1].strip('"')
        contents.setdefault(did, []).append(child)
        

for did, childs in contents.items():
    dids_to_attach = []
    for child in childs:
        dids_to_attach.append({'scope': 'cms', 'name': child})
    client.set_status(scope='cms', name=did, open=True)
    client.attach_dids(scope='cms', name=did, dids=dids_to_attach)
    client.set_status(scope='cms', name=did, open=False)


"""
CREATE replicas in T0_CH_CERN_Tape or any other RSE

INPUT: files.txt

/store/mc/RunIIFall18wmLHEGS/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/GEN-SIM/VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/90001/17F92391-80F9-9947-A700-6E10C2EFC6BF.root,253165146,bd624b5c
/store/mc/RunIIFall18wmLHEGS/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/GEN-SIM/VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/30000/6A5C5723-9B10-E346-B3E3-92DDEE46793C.root,2052471395,24ca1b15
/store/mc/RunIIFall18wmLHEGS/DYJetsToLL_M-105To160_VBFFilter_TuneCP5_PSweights_13TeV-amcatnloFXFX-pythia8/GEN-SIM/VBFPostMGFilter_102X_upgrade2018_realistic_v11_ext1-v1/30000/BB2A8199-56F7-6943-9A79-E2F0DCDA47FD.root,495981052,8b681234


"""
from rucio.client import Client
client = Client()


file_dids = [] # 4602 files
with open("./files.txt", "r") as files:
    for file in files:
        file_name = file.strip().split(',')[0]
        file_size = int(file.strip().split(',')[1])
        file_adler35 = file.strip().split(',')[2]
        client.add_replica(rse='T0_CH_CERN_Tape', scope='cms', name=file_name, bytes_=file_size, adler32=file_adler32)

