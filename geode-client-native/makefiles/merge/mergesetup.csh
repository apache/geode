
# parent of two baselines from JavaSoft
setenv baselines /avocet2/users/otisa/results/gemfireSrc

# current baseline
setenv old gemfireLinuxBase

# new baseline 
setenv new gemfire10maint_21nov

# directory for tools (such as this file) 
setenv tools $cwd

#  target of the merge ; we will merge changes between old and new into target
setenv target $cwd/../../

# name of log file produced by gdiff
setenv gdiffLog $cwd/gdiff_nov21.log

# name of script file produced by scanHs.pl
setenv mergeScript $cwd/merge_nov21.sh
