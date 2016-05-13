#!/bin/perl

#
# filter output from build on windows so error paths are reported with
# native windows paths..
#

# example:
#
# Entering directory '/cygdrive/c/...'
#
#   becomes
#
# Entering directory 'C:/'

open( LOG, ">buildWin.log" ) || die( "cannot create buildWin.log file" );

while( $line = <STDIN> ) {

  $line =~ s/(Entering|Leaving) directory\s+\`\/cygdrive\/(.)\//$1 directory \`$2:\//;
  if ( $line =~ m/Entering directory\s+\`(.*)\'/ ) {
    $makedir = $1;
  }
  if ( ! ($line =~ m/([a-z,A-Z]\:(.*\.(c|h)(p*))\(\d+\) : (error|warning))/ ) ) {
    $line =~ s/((.*)\(\d+\) : (error|warning))/${makedir}\/$1/;
  }
  print LOG $line;
  print $line;
}

