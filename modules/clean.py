import os 
import sys
import subprocess
import csv
from concurrent.futures import ThreadPoolExecutor


#######################clean libraries with fastp #############################


    ################# run fastp ###############

def clean_library(ID, R1_row, R2_row, clean_path, quality, threads):
    try:
        cmd = ['fastp', '-i', R1_row, '-o', os.path.join(clean_path, "clean_"+R1_row),
               '-q', quality, '-w', str(threads), '-h', os.path.join(clean_path, "reports", ID+".html")]

        if R2_row:
            cmd.extend(['-I', R2_row, '-O', os.path.join(clean_path, "clean_"+R2_row)])

        subprocess.run(cmd, check=True, stderr=subprocess.DEVNULL)

        print(f"Sample {ID} done")

    except subprocess.CalledProcessError:
        print(f"Error: Clean libraries fail for sample {ID} ...")
        raise Exception()

def clean_fastp(workdir, metadata_file, quality, threads):
    checkpoit_dir = os.path.join(workdir, "checkpoint")

    if not os.path.exists(checkpoit_dir):
        os.makedirs(checkpoit_dir)

    checkpoit_file = os.path.join(checkpoit_dir, "clean.done")

    if os.path.exists(checkpoit_file):
        print('''\nClean libraries done, skip this step\n''')
        return

    clean_path = os.path.join(workdir, "1.clean")

    if not os.path.exists(clean_path):
        os.makedirs(clean_path)

    if not os.path.exists(os.path.join(clean_path, "reports")):
        os.makedirs(os.path.join(clean_path, "reports"))

    print('''\nStep 1: Clean libraries, this can take a while ...\n''')
    
    t = int(threads)

    with ThreadPoolExecutor(max_workers=t//6) as executor:
        for row in metadata_file:
            ID = row['ID']
            R1_row = row['R1']
            R2_row = row.get('R2')
            executor.submit(clean_library, ID, R1_row, R2_row, clean_path, quality, threads)

    open(checkpoit_file, 'w').close()
    print('''\nDone\n''')