import os
import sys
import subprocess



def variant_call(workdir, genome):
    
    checkpoit_dir = os.path.join(workdir, "checkpoint")
                     
    checkpoit_file = os.path.join(checkpoit_dir, "variant_call.done")
    
    if os.path.exists(checkpoit_file):
        
        print('''\nVariant calling done, skip this step\n''' )
        
        return
    
    alignment_path = os.path.join(workdir, "3.bwa_aligment")
    
    ##################################### Variant calling ############################
    
    print('''\nStep 6: Variant calling\n''')
    
    vcf_path = os.path.join(workdir, "4.variant_call")
    
    if not os.path.exists(vcf_path):
        os.makedirs(vcf_path)
        
        
    ############################### freebayes ######################
    list_bam_path = os.path.join(workdir, "list_bam")
    
    try:
        with open(list_bam_path, 'w') as list_bam_file:
            for bam_file in os.listdir(alignment_path):
                if bam_file.endswith("markdup.bam"):
                    list_bam_file.write(os.path.join(alignment_path, bam_file) + "\n")
        
        freebayes_cmd = [
            'freebayes',
            '--gvcf',
            '-f', genome,
            '-L', list_bam_path,
            '-v', os.path.join(vcf_path, "variant.vcf")
        ]
        
        subprocess.run(freebayes_cmd, check=True)
        
    except subprocess.CalledProcessError:
        print('''\nVariant calling fail, EXIT ...\n''')
        raise Exception("Variant calling fail")
    
    open(checkpoit_file, 'w').close()
    print('''\nDone\n''')
    
    
        