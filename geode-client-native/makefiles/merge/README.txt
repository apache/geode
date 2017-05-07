
# instructions for merging Sun hotspot sources into our VM sources

1. edit mergesetup.csh for the locations of the old and new baselines, etc

2. run do_diff.sh

3. examine the resulting $mergeScript file for
  unwanted or missing directory additions/removals
  and unwanted or missing cvs add/remove 
  edit as required.  You may want to also run dircomp.sh

  for removed files, make sure entries are removed from these hotspot files:
      includeDB* , vm.dsp

4. chmod +x the file $mergeScript

5. run  do_merge.sh

6. edit vm/hotspot20/build/solaris/makefiles/vm.make  and
        vm/hotspot20Win/build/win32_i486/makefiles/vm.make

   for the HOTSPOT_BUILD_VERSION to match the output from  
     java -version   run with Sun's binary release .
