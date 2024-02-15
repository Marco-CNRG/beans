#!/usr/bin/env python

import os
import sys
import argparse
import csv
from modules import clean
from modules import bwa_alignment
from modules import sam_to_bam
from modules import variant_calling
from modules import gatk_variant_calling


parser = argparse.ArgumentParser(description= "Thank you for use Bioinformatic envioriment for variant analysis SNPs")
parser.add_argument('--output_dir', '-o', type=str, help= " Output directory", required=False)
parser.add_argument('--metadata', '-m', type=str, help= "Metadata file", required=True)
parser.add_argument('--skip_clean', '-s', action='store_true', help="Skip cleaning step")
parser.add_argument('--threads', '-t', type=str, default='4', help= "Number of CPUs (Optional)")
parser.add_argument('--quality', '-q', type=str, default='20', help= "Phread Quality (Optional)")
parser.add_argument('--genome', '-g', type=str, help="Reference genome", required=True)
parser.add_argument('--method', '-M', type=str, choices=['freebayes', 'gatk'], help="Method to use: freebayes or gatk", required=False, default='freebayes' )

args = parser.parse_args()


########Files exist? ################
metadata_path = os.path.join(os.getcwd(), args.metadata)

def read_metadata(metadata_path):
    metadata_file_read = []
    
    try:
        with open(metadata_path, mode= 'r', newline='') as file:
            tsv_reader = csv.DictReader(file, delimiter= '\t')
            for row in tsv_reader:
                metadata_file_read.append(row)
                
        return metadata_file_read
    
    except:
        print("metadata: No such file or directory")
        sys.exit()


metadata_file = read_metadata(metadata_path)


not_exist = []
for row in metadata_file:
    R1 = row['R1']
    R2 = row.get('R2')
        
    if not os.path.exists(R1):
        not_exist.append(R1)
        
    if R2:
        if not os.path.exists(R2):
            not_exist.append(R2) 

if not_exist:
    print(f"Files {not_exist} do not exists")
    sys.exit()
    

#####Is a fasta file? ######

fasta_found = False  

with open(args.genome, 'r') as file:
    for line_num, line in enumerate(file, 1):
        if line_num > 10:
            break
        line = line.strip()
        if line and line.startswith(">"):            
            fasta_found = True  
            break 

if not fasta_found:
    print('''\nGenome file is not a fasta file\n''')
    sys.exit()
    
                         
                



def create_workdir(output_dir):
    workdir_dir = os.path.join(os.getcwd(), output_dir)
    
    if not os.path.exists(workdir_dir):
        os.makedirs(workdir_dir)
        

        
        

create_workdir(args.output_dir)


workdir = os.path.join(os.getcwd(), args.output_dir)






checkpoint_dir = os.path.join(workdir, "checkpoint")


try:
    if not args.skip_clean:
        #clean.q_fastqc(workdir, metadata_file, args.threads)

        clean.clean_fastp(workdir, metadata_file, args.quality, args.threads)

        #clean.q_fastqc_end(workdir, metadata_file, args.threads)       
        


    bwa_alignment.index_bwa(workdir, args.genome)

    bwa_alignment.bwa_alignment(workdir, metadata_file, args.threads, args.skip_clean)

    sam_to_bam.sam_to_bam(workdir, args.threads, metadata_file)

    
    if args.method == 'gatk':
        print("gatk")
        
        gatk_variant_calling.markd_gatk(workdir, args.genome, metadata_file, args.threads)
        gatk_variant_calling.haplotypeCaller(workdir, metadata_file, args.genome, args.threads)
        gatk_variant_calling.genotype(workdir, metadata_file, args.genome, args.threads)
        
        
    if args.method == 'freebayes':   
    
        sam_to_bam.markd(workdir, metadata_file, args.threads)
    
        variant_calling.variant_call(workdir, args.genome)
    
    
    
except Exception as e:
    print(f"{e}")
    
