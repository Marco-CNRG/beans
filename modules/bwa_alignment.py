import os
import sys
import subprocess

####################4. bwa index ##########################################################################
def index_bwa(workdir, genome):
    checkpoit_dir = os.path.join(workdir, "checkpoint")
    
    if not os.path.exists(checkpoit_dir):
        os.makedirs(checkpoit_dir)
            
    checkpoit_file = os.path.join(checkpoit_dir, "bwa_index.done")
    
    if os.path.exists(checkpoit_file):
        
        print('''\nBWA Index done, skip this step\n''' )
        
        return
    

    ################### mkdirs 4.index_bwa###############
    
    index_path = os.path.join(workdir, "2.bwa_index")
    
    if not os.path.exists(index_path):
        os.makedirs(index_path)
        
                      
                    ##############If genome is a fasta file create index ##############
    print('''\nStep 2: Create a BWA index.\n''')
                    
    try:
        subprocess.run(['bwa-mem2', 'index', '-p', os.path.join(index_path, "index_bwa"), genome],
                        check=True)
                                              
                    
    except subprocess.CalledProcessError:
        print( '''\nIndex fails, EXIT ...\n''' )
        raise Exception("Index fails")
                                                                
                    
    open(checkpoit_file, 'w').close()
    return                
    
            

################################5. bwa_alignment ##################

def bwa_alignment(workdir, metadata_file, threads, skip_clean=False):
    checkpoit_dir = os.path.join(workdir, "checkpoint")
    
   
                  
    checkpoit_file = os.path.join(checkpoit_dir, "bwa_alignment.done")
    
    if os.path.exists(checkpoit_file):
        
        print('''\nBWA alignments done, skip this step\n''' )
        
        return
    
    alignment_path = os.path.join(workdir, "3.bwa_aligment")
    
    if not os.path.exists(alignment_path):
        os.makedirs(alignment_path)
        
    
    
    ##################5. bwa alignments ##################
    
    print('''\nStep 3: BWA alignment\n''')
    
    
    for row in metadata_file:
        id_sample = row['ID']
        R1_row = row['R1']
        R2_row = row.get('R2')
    
        if skip_clean:
            
            data = os.path.join(os.getcwd())
            
            try:
                cmd = ['bwa-mem2', 'mem', '-t',threads, '-R', f'@RG\\tID:{id_sample}\\tSM:{id_sample}',
                       '-o', os.path.join(alignment_path, id_sample+".sam"),
                                    os.path.join(workdir,"2.bwa_index", "index_bwa"),
                                    os.path.join(data, R1_row)]
                                
                if R2_row:
                    cmd.extend([os.path.join(data, R2_row)])
                
                subprocess.run(cmd, check=True)
                
            except subprocess.CalledProcessError:
                            
                        print('''\nBWA alignment fail, EXIT ...\n''')
                        raise Exception("BWA alignment fail")
                        
                        
            open(checkpoit_file, 'w').close()
            print('''\nDone\n''')
            
        else:
            data = os.path.join(workdir, "1.clean")
            
            try:
                cmd = ['bwa-mem2', 'mem', '-t',threads, '-R', f'@RG\\tID:{id_sample}\\tSM:{id_sample}',
                       '-o', os.path.join(alignment_path, id_sample+".sam"),
                                        os.path.join(workdir,"2.bwa_index", "index_bwa"),
                                        os.path.join(data, "clean_"+R1_row)]
                
                if R2_row:
                    cmd.extend([os.path.join(data, "clean_"+R2_row)])
                    
                subprocess.run(cmd, check=True)
            
            except subprocess.CalledProcessError:
                            
                    print('''\nBWA alignment fail, EXIT ...\n''')
                            
                    raise Exception("BWA alignment fail")
                
                        
            open(checkpoit_file, 'w').close()
            print('''\nDone\n''')