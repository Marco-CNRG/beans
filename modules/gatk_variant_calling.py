import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

def cmd_markdup(alignment_path, ID, threads):
    cmd = ['gatk', '--java-options', f'-XX:+UseParallelGC -XX:ParallelGCThreads={threads//2}',
                'MarkDuplicates', '--CREATE_INDEX', 
                '-I', os.path.join(alignment_path, ID + ".bam"), 
                '-M', os.path.join(alignment_path, ID + ".metrics"),
                '-O', os.path.join(alignment_path, ID + "_markdup.bam")]
        
                
    try:
            
        subprocess.run(cmd, check=True)
                
    except subprocess.CalledProcessError:
            
        raise Exception('''\nMark duplicates fail, EXIT ...\n''')
    

def markd_gatk(workdir, genome, metadata_file, threads):
    checkpoit_dir = os.path.join(workdir, "checkpoint")
                     
    checkpoit_file = os.path.join(checkpoit_dir, "markd_gatk.done")
        
        
    alignment_path = os.path.join(workdir, "3.bwa_aligment")
    
    
    check_dict = os.path.join(checkpoit_dir, "dictionary.done")
    
    if not os.path.exists(check_dict):
        
        try:
                subprocess.run(['gatk', 'CreateSequenceDictionary', '-R', genome], 
                                check= True,
                                stderr=subprocess.DEVNULL)
                
                subprocess.run(['samtools', 'faidx', genome], check=True)
                
        except subprocess.CalledProcessError:
                raise Exception('''\nDictionary fail, EXIT ...\n''')
            
        open(check_dict, 'w').close() 
        
        
    if os.path.exists(checkpoit_file):
            
        print('''\nMark duplicates done, skip this step\n''' )
            
        return
    
    print('''\nMark duplicates\n''')
    
        
    ########Markdup#########################################
    t = int(threads)
    
    with ThreadPoolExecutor(max_workers= t//2) as executor:    
    
        for row in metadata_file:
            ID = row['ID']
            
            executor.submit(cmd_markdup, alignment_path, ID, threads)   
        
        
    open(checkpoit_file, 'w').close()
    
    
    
def cmd_haplotype(genome, ID, alignment_path, haplotype_path, threads):
    
        cmd = ['gatk', '--java-options', f'-XX:+UseParallelGC -XX:ParallelGCThreads={threads//2}',
                'HaplotypeCaller',
                '-ERC', 'GVCF', 
                '-I', os.path.join(alignment_path, ID + "_markdup.bam"),                 
                '-O', os.path.join(haplotype_path, ID + ".g.vcf.gz"),
                '-R', os.path.join(genome)]        
                
        try:
            
            subprocess.run(cmd, check=True)
                
        except subprocess.CalledProcessError:
            
            raise Exception('''\nHaplotype caller fail, EXIT ...\n''')
        
    
    
    
def haplotypeCaller(workdir,metadata_file, genome, threads):
    checkpoit_dir = os.path.join(workdir, "checkpoint")
                     
    checkpoit_file = os.path.join(checkpoit_dir, "haplotype_caller.done")
        
    if os.path.exists(checkpoit_file):
            
        print('''\nHaplotype caller done, skip this step\n''' )
            
        return
        
    alignment_path = os.path.join(workdir, "3.bwa_aligment")
    
    
    haplotype_path = os.path.join(workdir, "4.Haplotype_caller")
    
    if not os.path.exists(haplotype_path):
        os.makedirs(haplotype_path)
    
    
    print('''\nHaplotype caller\n''')  
    
    
    ########Haplotype Caller#########################################
    t = int(threads)  
    
    
    with ThreadPoolExecutor(max_workers=t//2) as executor:            
        for row in metadata_file:
            ID = row['ID']            
            executor.submit(cmd_haplotype, genome, ID, alignment_path, haplotype_path, threads)
    
    
    open(checkpoit_file, 'w').close()
    
    
    
    
def genotype(workdir, metadata_file, genome, threads):
    checkpoit_dir = os.path.join(workdir, "checkpoint")
                     
    checkpoit_file = os.path.join(checkpoit_dir, "genotype_gatk.done")
        
    if os.path.exists(checkpoit_file):
            
        print('''\nGenotype done, finish\n''' )
            
        return
     
    
    haplotype_path = os.path.join(workdir, "4.Haplotype_caller")
    
    genotype_path = os.path.join(workdir, "5.Genotype")
    
    
    if not os.path.exists(genotype_path):
        os.makedirs(genotype_path)
        
    
    gvcfs = []
    
    for row in metadata_file:
        ID = row['ID']
        
        gvcfs.append(f'-V {os.path.join(haplotype_path, ID + ".g.vcf.gz")}')
        
        
    gvcfs_argument = ' '.join(gvcfs)
    
   
    
    cmd = ['gatk', 'CombineGVCFs', '-R', genome, '-O', os.path.join(genotype_path, 'combine_GVCFs.g.vcf.gz'), gvcfs_argument]
    
    cmd_combine = ' '.join(cmd)
    
    print(cmd_combine)
    
    try:
        subprocess.run(cmd_combine, shell=True, executable='/bin/bash',
                       check=True)
        
    except subprocess.CalledProcessError:
        raise Exception('''\nCombine GVCFs fail, EXIT ...\n''')
    

    cmd_genotype = ['gatk', '--java-options', f'-XX:+UseParallelGC -XX:ParallelGCThreads={threads}',
                'GenotypeGVCFs',                 
                '-V', os.path.join(genotype_path, "combine_GVCFs.g.vcf.gz"),                 
                '-O', os.path.join(genotype_path, "genotype_results.vcf.gz"),
                '-R', os.path.join(genome)]
    
    try:
         subprocess.run(cmd_genotype, check=True)

    except subprocess.CalledProcessError:
         raise Exception('''\nGenotype fail, EXIT ...\n''')
    
    open(checkpoit_file, 'w').close()
    

    
    
    
