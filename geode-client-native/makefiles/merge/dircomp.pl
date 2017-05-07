#! /usr/bin/perl

# HSBASE is the current baseline

$HSBASE=$ENV{'baselines'}$ENV{'old'}

#  js is the new baseline
$js=$ENV{'baselines'}$ENV{'new'}

# not yet ready to use, don't know wher find.pl used to be 
require "find.pl";

# ---------------------------------------------------------------
# Get the directories from the base release
%jsNames = ();
chdir($js) || die "cannot chdir to $js: $!";
eval 'sub wanted {&wanted_js_dirs;}';
&find('.');

# Get the directories from our tree.
%myNames = ();
chdir("$HSBASE") || die "cannot chdir to $HSBASE : $!";
eval 'sub wanted {&wanted_js_dirs;}';
&find('.');

# remove duplicates from both lists
foreach $each (keys(%myNames)) {
  next if !defined($jsNames{$each});
  delete $jsNames{$each};
  delete $myNames{$each};
  }

# Now report
@jsNames = keys(%jsNames);
@jsNames = sort(@jsNames);
print "-- Directories in $js but not in $HSBASE :\n";
if ($#jsNames >= 0) {
  foreach $each (@jsNames) {
    print "$each\n";
    }
  }
else {
  print "  (NONE) \n";
  }
@myNames = keys(%myNames);
@myNames = sort(@myNames);
print "-- Directories in $HSBASE but not in $js :\n";
if ($#myNames >= 0) {
  foreach $each (@myNames) {
    print "$each\n";
    }
  }
else {
  print "  (NONE) \n";
  }

# ---------------------------------------------------------------
# Now, for the common directories, scrutinize the files

# Get the files from the base release
%jsNames = ();
chdir($js) || die "cannot chdir to $js: $!";
eval 'sub wanted {&wanted_js_files;}';
&find('.');

# Get the files from our tree.
%myNames = ();
chdir("$HSBASE") || die "cannot chdir to $HSBASE $!";
eval 'sub wanted {&wanted_js_files;}';
&find('.');

@jsNames = keys(%jsNames);
@jsNames = sort(@jsNames);

@myNames = keys(%myNames);
@myNames = sort(@myNames);

# remove duplicates from both lists
foreach $each (keys(%myNames)) {
  next if !defined($jsNames{$each});
  delete $jsNames{$each};
  delete $myNames{$each};
  }

# Now report
print "-- Files in JavaSoft but not in $HSBASE \n";
if ($#jsNames >= 0) {
  foreach $each (@jsNames) {
    print "$each\n";
    }
  }
else {
  print "  (NONE) \n" ;
  }
print "-- Files in $HSBASE but not in JavaSoft:\n";
if ($#myNames >= 0) {
  foreach $each (@myNames) {
    print "$each\n";
    }
  }
else {
  print "  (NONE) \n" ;
  }



# ---------------------------------------------------------------

exit 0;

sub wanted_js_dirs {
  return if !(($dev,$ino,$mode,$nlink,$uid,$gid) = lstat($_));
  return if ! -d _;
  $jsNames{$name} = 1;
  }
 
sub wanted_archbase_dirs {
  return if !(($dev,$ino,$mode,$nlink,$uid,$gid) = lstat($_));
  return if ! -d _;

  # only CVS directories
  return if ! -d "$_/CVS";

  # Recognize GemStone specific directories (
  return if $name =~ m#^\./build/adlc\b#;
  return if $name =~ m#^\./build/bin\b#;
  return if $name =~ m#^\./build/debug\b#;
  return if $name =~ m#^\./build/incls\b#;
  return if $name =~ m#^\./build/release\b#;

  $myNames{$name} = 1;
  }

sub wanted_js_files {
  return if !(($dev,$ino,$mode,$nlink,$uid,$gid) = lstat($_));
  return if ! -f _;

  $jsNames{$name} = 1;
  }

sub wanted_archbase_files {
  return if !(($dev,$ino,$mode,$nlink,$uid,$gid) = lstat($_));
  return if ! -f _;

  return if $name =~ m#^.*/CVS/.*#;
  return if $name =~ m#^.*/CClassHeaders/.*#;

  return if $name =~ m%/\.#.*%;
  return if $name =~ m%/\.class\.headers\.sparc%;
  return if $name =~ m#/\.cvsignore\b#;
  return if $name =~ m#/\.bin\.dirs\b#;
  return if $name =~ m#/\.classes\.list\b#;
  return if $name =~ m#/\.lib\.dirs\b#;

  return if $name =~ m#^\./build/ADLCompiler.*#;
  return if $name =~ m#^\./build/Dependencies*#;
  return if $name =~ m#^\./build/i486.ad*#;
  return if $name =~ m#^\./build/includeDB*#;
  return if $name =~ m#^\./build/includeDB.*#;
  return if $name =~ m#^\./build/grep*#;
  return if $name =~ m#^\./build/Makefile*#;
  return if $name =~ m#^\./build/vm.*#;
  return if $name =~ m#^\./build/*.txt#;

  return if $name =~ m#^\./src/grep*#;
  return if $name =~ m#^\./src/*.txt#;
  return if $name =~ m#^\./src/share/vm/prims/gri*#;

  $myNames{$name} = 1;
  }

