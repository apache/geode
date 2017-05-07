#!/usr/bin/perl -w

use strict;
use vars qw ($CDBPATH $OUTPUT_DIR $THREAD_UNDERSCORE $THREAD_DEMARCATOR $PID $CDB @ARGV %ENV);
use POSIX;

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

$OUTPUT_DIR = ".";

$THREAD_UNDERSCORE = "\\n**************\\n";
$THREAD_DEMARCATOR = "\\n\\n=======================================================================\\n\\n";

my ($sysname, $nodename, $release, $version, $machine) = POSIX::uname();

if (exists $ENV{CDB}) {
  if ($sysname =~ /^CYGWIN.*$/) {
    $CDBPATH = `cygpath "$ENV{CDB}"`;
    chomp $CDBPATH;
  } else {
    $CDBPATH = $ENV{CDB};
  }
} else {
    $CDBPATH = `cygpath "$ENV{GFCPP}/../framework/bin"`;
    chomp $CDBPATH;
}
$ENV{PATH} .= ':' . $CDBPATH;

$CDB = "cdb.exe";

my $COMMAND_FILE = $OUTPUT_DIR . "/" . "cdbcomm." . $PID;

open(COMM, ">$COMMAND_FILE") || do {
  print "could not open the command file $COMMAND_FILE";
  exit 1;
};
print COMM "~\n";
print COMM "q\n";

close COMM;

my $CDBCOMMAND = "$CDB -p $PID -pd -cf $COMMAND_FILE";

my @thread_info = `$CDBCOMMAND`;

open(COMM, ">$COMMAND_FILE") || do {
  print "could not open the command file $COMMAND_FILE";
  exit 1;
};

foreach my $thr_line(@thread_info) {
  if ($thr_line =~ /\s+(\d+)\s+Id:\s*([^\s]+).+/i) {
    my $thrd = $1;
    my $thrd_id = $2;
    my @id_split = split('\.', $thrd_id);
    my $pid = hex($id_split[0]);
    $thrd_id = hex($id_split[1]);
    print COMM ".printf \"$THREAD_UNDERSCORE\\n\"\n";
    print COMM ".printf \"THREAD $thrd [PID $pid, ID $thrd_id]\\n\"\n";
    print COMM ".printf \"$THREAD_UNDERSCORE\\n\"\n";
    print COMM "~${thrd}e kp\n";
    print COMM ".printf \"$THREAD_DEMARCATOR\\n\"\n";
  }
}
print COMM "q\n";
 
close COMM;

my @output = `$CDBCOMMAND`;

my $start = 0;

foreach my $out_line(@output) {
  if ($out_line =~ /^[0-9]+:[0-9]+\>/) {
    $start = 1;
  }
  elsif ($start == 1 && $out_line !~ /^quit:/) {
    chomp $out_line;
    print $out_line . "\r\n";
  }
}

unlink $COMMAND_FILE;
