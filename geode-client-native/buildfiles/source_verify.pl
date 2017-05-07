my %opts;
use strict;
use Getopt::Long;

my %errorMessages = (
  "sprintf"                              => " Never use sprintf.  Use ACE_OS::snprintf",
  "new"                                  => " Never use new. Use GF_NEW",
  "delete"                               => " Never use delete. Use GF_SAFE_DELETE or GF_SAFE_DELETE_ARRAY",  
  "strcpy"                               => " Almost always, snprintf is better than strcpy",
  "strcat"                               => " Almost always, snprintf is better than strcat", 
  "sscanf"                               => " sscanf can be ok, but is slow and can overflow buffers",
  "asctime"                              => " Consider using asctime_r which is multithreaded safed ",
  "ctime"                                => " Consider using ctime_r which is multithreaded safed ",
  "getgrgid"                             => " Consider using getgrgid_r which is multithreaded safed ",
  "getgrnam"                             => " Consider using getgrnam_r which is multithreaded safed ",
  "getlogin"                             => " Consider using getlogin_r which is multithreaded safed ",
  "getpwnam"                             => " Consider using getpwnam_r which is multithreaded safed ",
  "getpwuid"                             => " Consider using getpwuid_r which is multithreaded safed ",
  "gmtime"                               => " Consider using gmtime_r which is multithreaded safed ",
  "localtime"                            => " Consider using localtime_r which is multithreaded safed ",
  "rand"                                 => " Consider using rand_r which is multithreaded safed ",
  "readdir"                              => " Consider using readdir_r which is multithreaded safed ",
  "strtok"                               => " Consider using strtok_r which is multithreaded safed ",
  "ttyname"                              => " Consider using ttyname_r which is multithreaded safed ",
  "main::CheckForTabs"                   => " Tab found, use spaces ",
  "main::CheckForWhiteSpaceInEndOfLine"  => " Line ends in whitespace.  Consider deleting these extra spaces",    
);

my @restrictedUsage = (    "sprintf", "new","delete","strcpy","strcat","scanf",
                           "asctime", "ctime","getgrgid", "getgrnam", "getlogin", "getpwnam",
                           "getpwuid", "gmtime", "localtime", "rand", "readdir",
                       "strtok", "ttyname");

sub usage {
  print "\n";
  print "$0 --file <filename>\n";
  print "   file:   Name of a file that needs to be verify\n";
  print "\n";
  print "$0 --directory <dirname>\n";  
  print "   directory:   Name of a directory whose files needs to be verify\n";
  exit(1);
}

sub printError {
  my ($errorCode, $fname, $line) = @_;  
  printf ( "$fname:$line:  ".$errorMessages{$errorCode}."\n" );    
}

sub CheckForTabs {
  my ($line, $fname, $linenum) = @_;    
  if ( $line !=~ /\".*\t.*\"/ ) {
    if( $line =~ /\t/ ) {
      printError((caller(0))[3] ,$fname,$linenum);
    }
  }  
}

sub CheckForWhiteSpaceInEndOfLine {
  my ($line, $fname, $linenum) = @_;  
  if ( $line =~ /\s+$/ ) {
    printError((caller(0))[3],$fname,$linenum);
  }
}

sub CheckForRestrictedUsage {
  my ($line, $fname, $linenum, $apiname) = @_;  
  if ( $line =~ /\b$apiname\b/ ) {
    printError($apiname,$fname,$linenum);
  }    
}



sub parseFileForErrors {
  my $fname = shift;
  open( FILE, $fname ) or die "Can't open $fname: $!";
  my @lines = <FILE>;
  close(FILE);
  my $linenum = 0;
  foreach my $_(@lines) {
    $linenum++;
    chomp;
    # Do not read the lines which has 
    # C or C++ style comments.
    next if m{^\s*//};
    # Comment of C style
    next if m{^\s*/\*.*\*/\s*$};
    # Continue till the comment is not finish
    next while ( m{^\s*/\*}..m{^\s*\*/} );
    # Ignore blank lines
    next if m{/^\s*$/};
    # ignore lines with only spaces or tabs only
    next if $_ !~ /\S/;
    
    CheckForTabs($_,$fname, $linenum);
    CheckForWhiteSpaceInEndOfLine($_,$fname, $linenum);
    foreach my $apiname ( @restrictedUsage ) {
      CheckForRestrictedUsage($_,$fname, $linenum, $apiname);
    }    
  }    
}

sub parseDirForErrors {
  
  my $dirname = shift;
  chomp $dirname;
  
  opendir(DIR,$dirname);
  my @files = readdir(DIR);
  closedir(DIR);  
  
  foreach my $fname (@files) {
    next if $fname eq '.';
    next if $fname eq '..';
    next if $fname eq 'GNUmakefile';
    next if (-d $fname);          
    parseFileForErrors($dirname."/".$fname);    
  }
}


my $fileToScan = undef;
my $dirToScan  = undef;

my $rc = GetOptions (
  "directory=s" =>  sub { $dirToScan  = pop(@_); },
  "file=s"      =>  sub { $fileToScan = pop(@_); },
  "help"        =>  sub { usage(); },
);

if ( defined $fileToScan )  {
  parseFileForErrors($fileToScan);
} elsif ( defined $dirToScan ) {
  parseDirForErrors($dirToScan);
} else {
  usage();
}


