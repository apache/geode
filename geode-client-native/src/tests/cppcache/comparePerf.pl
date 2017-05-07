#!/bin/perl

# Compare PerfSuite lines in one file against those in another...

$argOne = shift( @ARGV );
$argTwo = shift( @ARGV );

$argOne =~ s/\\/\//g;
$argTwo =~ s/\\/\//g;

# Read baseline

if ( ! -f "$argOne" ) {
  print "Error: No baseline file to for host, checkin appropriate testPutGetPerf.out as file $argOne\n";
  exit( 1 );
}

open( FILONE, "grep PerfSuite $argOne|" ) || die( "Error: No baseline file to for host, checkin appropriate testPutGetPerf.out as file $argOne" );

while( $line = <FILONE> ) {
  $line =~ m/PerfSuite\] (\w+) -- (\d+) ops/;
  $key = $1;
  $value = $2;
  $dataone{"${key}"} = $value;
}

close( FILONE );

@tmp = keys( %dataone );
if ( $#tmp == -1 ) {
  print "Error: No data found in $argOne\n";
  exit( 1 );
}

# Read new results

if ( ! -f "$argTwo" ) {
  print "Error: No result file from test to compare, expected $argTwo\n";
  exit( 1 );
}

open( FILTWO, "grep PerfSuite $argTwo|" ) || die( "No file $argTwo" );

while( $line = <FILTWO> ) {
  $line =~ m/PerfSuite\] (\w+) -- (\d+) ops/;
  $key = $1;
  $value = $2;
  $datatwo{"${key}"} = $value;
}

close( FILTWO );

$code = 0;

@tmp = keys( %datatwo );
if ( $#tmp == -1 ) {
  print "Error: No data found in $argTwo\n";
  exit( 1 );
}

# display comparisons

format top =
%CHANGE  TESTCASE                     BASELINE CURRENT
======== ============================ ======== ================
.
format STDOUT =
( @##% ) @<<<<<<<<<<<<<<<<<<<<<<<<<<< @####### @####### ops/sec
$percent, $item,                       $baseValue, $newValue
.

foreach $item (sort( keys( %dataone ) )) {
  $change = $datatwo{"${item}"} - $dataone{"${item}"};
  $percent = int( 100 * ( $change / $dataone{"${item}"} ));
  $baseValue = $dataone{"${item}"};
  $newValue = $datatwo{"${item}"};
  write;
  if ( $percent < ( -5 ) ) {
    $code++;
  }
}

if ( $code > 0 ) {
  print "Error: $code performance cases failed by more than 5%\n";
}

exit( $code );
