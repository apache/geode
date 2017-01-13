#!/usr/bin/perl -w

use File::Basename;

use lib dirname(__FILE__);

use strict;

use vars qw ($OUTPUT_DIR $OUTPUT_FILE $NL $THREAD_UNDERSCORE $THREAD_DEMARCATOR $PID $GDB @ARGV
             $USESTDOUT );

$PID = $ARGV[0];
if ( ! defined $PID ) {
  print "Invalid argument: The script takes pid as argument\n";
  exit 1;
}

chomp $PID;

if ($PID !~ /^\d+$/) {
  print "Invalid argument: The script takes pid as argument\n";
  exit 1;
}

# Default behaviour .... output in a file
$USESTDOUT = 0;

if ( scalar(@ARGV) > 1 ) {
  $USESTDOUT = 1;
}


$OUTPUT_DIR = ".";
$OUTPUT_FILE = $OUTPUT_DIR . "/" . "gdbout." . $PID;

$THREAD_UNDERSCORE = "**************";
$THREAD_DEMARCATOR = "=======================================================================";
$NL = "\n";

require GDB;

$GDB = "gdb -p";

our $gdb = new Devel::GDB (-execfile => "$GDB $PID") ;

my $INFO_THREADS = $gdb->get("info threads");

my @threads = split '\n', $INFO_THREADS;

my $numOfThreads = scalar @threads;

if ( $numOfThreads > 0 ) {
  
  my $outstring = "";

  $outstring .= $NL . $THREAD_DEMARCATOR . $NL;
  foreach my $thr_line(@threads) {
    $outstring .= $thr_line .  $NL;
  }
  $outstring .=  $THREAD_DEMARCATOR . $NL;
  foreach my $thr_line(@threads) {
    #  1 Thread -1218576256 (LWP 12154
    if ($thr_line =~ /\s+(\d+)\s+Thread\s+(\d+).+/) {
      my $thrd = $1;
      my $thrd_id = $2;
      $gdb->get("thread $thrd");
      my $thrd_bt = $gdb->get("bt") ;
      $outstring .= "THREAD " . $thrd . " " . $thrd_id . $NL . $THREAD_UNDERSCORE . $NL . $thrd_bt . $THREAD_DEMARCATOR . $NL;
    }
  }
 
  if ($USESTDOUT == 0)
  {
    open(OUT, ">$OUTPUT_FILE") || do {
      print "could not open the output file $OUTPUT_FILE";
      $gdb -> get ( 'detach' ) ;
      exit 1;
    };

    print OUT $outstring;
    close OUT;
  } else
  {
    print STDOUT $outstring;
  }
}

$gdb -> get ( 'detach' ) ;
