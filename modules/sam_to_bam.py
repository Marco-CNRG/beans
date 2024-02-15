import os
import sys
import subprocess


def sam_to_bam(workdir, threads, metadata_file):
    
    checkpoit_dir = os.path.join(workdir, "checkpoint")
                     
    checkpoit_file = os.path.join(checkpoit_dir, "bam_convert.done")
    
    if os.path.exists(checkpoit_file):
        
        print('''\nBAM convertion done, skip this step\n''' )
        
        return
    
    alignment_path = os.path.join(workdir, "3.bwa_aligment")
    
    
    #####Convert sam to bam and sort###################
    
    print('''\nStep 4: SAM to BAM\n''')
    
    for row in metadata_file:
        ID = row['ID']
        
        
        cmd_view = ['samtools', 'view','-@', threads, '-u', os.path.join(alignment_path,ID+".sam")]
        cmd_sort = ['samtools', 'sort', '-@', threads]
        cmd_convert = ['samtools', 'view', '-@', threads, '-o', os.path.join(alignment_path,ID+".bam")]
        
                
        command_view = ' '.join(cmd_view)
        command_sort = ' '.join(cmd_sort)
        command_convert = ' '.join(cmd_convert)
        
        full_command = f"{command_view} | {command_sort} | {command_convert}"
        
        try:
            subprocess.run(full_command, shell=True, executable='/bin/bash',
                           check=True,
                           stderr=subprocess.DEVNULL)
            
        except subprocess.CalledProcessError:
            
            print('''\nConvertion fail, EXIT ...\n''')
            
            raise Exception("Convertion fail")
        
        subprocess.run(['rm', os.path.join(alignment_path,ID+".sam")])
        
        print('''\n\tFile ''' + ID + ''' convertion done\n''')
        
    open(checkpoit_file, 'w').close()
    print('''\nDone\n''')
    
    
def markd(workdir, metadata_file, threads):
    checkpoit_dir = os.path.join(workdir, "checkpoint")
                     
    checkpoit_file = os.path.join(checkpoit_dir, "markd.done")
        
    if os.path.exists(checkpoit_file):
            
        print('''\nMark duplicates done, skip this step\n''' )
            
        return
        
    alignment_path = os.path.join(workdir, "3.bwa_aligment")
        
        ################ Mark duplicates #########################
    print('''\nStep 5: Mark duplicates\n''')
        
        
    for row in metadata_file:
        ID = row['ID']
        
        try:
            subprocess.run(['sambamba', 'markdup', '-t', threads, os.path.join(alignment_path, ID+".bam"), 
                            os.path.join(alignment_path, ID+"_markdup.bam")],
                           check=True)
            
        except subprocess.CalledProcessError:
            print('''\nMark duplicates fail, EXIT ...\n''')
            raise Exception("Mark duplicates fail")
        
    open(checkpoit_file, 'w').close()
    print('''\nDone\n''')
    