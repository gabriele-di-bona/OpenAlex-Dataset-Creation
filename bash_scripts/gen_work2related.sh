#!/bin/bash
#$ -cwd
#$ -t 66-4516
#$ -j y
#$ -pe smp 1 
#$ -l h_vmem=40G
# #$ -l highmem
#$ -l h_rt=1:0:0
#$ -l rocky
# #$ -m bae
# #$ -o /data/scratch/ahw701/FluctuatingDiscoveries/outputs/output

# module load anaconda3
# conda activate gt
module load miniforge
conda activate gt_rocky
export OMP_NUM_THREADS=1
# this bash script is supposed to be run from the subfolder outputs, so that the output goes directly there
cd ../../python_scripts/

python 12_1_gen_work2related.py -ID ${SGE_TASK_ID}
