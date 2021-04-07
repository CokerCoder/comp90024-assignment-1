#!/bin/bash

#SBATCH --partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4

module load foss/2019b
module load python/3.7.4

time srun -n 8 python3 code.py