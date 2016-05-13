#! /usr/bin/perl
#
#   for removed files, make sure entries are removed from these files:
#       includeDB* , vm.dsp
#   then remove all src/* files from the PC and re-copy everything from unix,
#   on the pc remove all .obj files and incls to ensure full rebuild.
#

$tools=$ENV{'tools'}; 
$baselines = $ENV{'baselines'};
$old = $ENV{'old'};		# existing JavaSoft baseline
$new = $ENV{'new'};		# new JavaSoft baseline
$gdiffLog = $ENV{'gdiffLog'};

$target = $ENV{'target'};

open(F, "< $gdiffLog ") || die "cannot open diff log";

# ------------- end of stuff to edit for setup of the merge

select(STDOUT); $| = 1;

#print "#! /usr/bin/bash -xv\n";
print "#! /usr/bin/bash\n";
print "old=$baselines/$old\n";
print "new=$baselines/$new\n";
print "target=$target\n";

($oldPat = $old) =~ s/\./\\./g;
($newPat = $new) =~ s/\./\\./g;

%removed_dirs = ();

for (;;) {
  $line = <F>;
  last if !defined($line);
  chop($line);

  if ($line =~ m@^diff -wBrc $oldPat/(\S+).*@) {
    # exists in both; do a merge
    $line = $1;
    if (-l "$old/$line") {
      print "# link: $line\n";
      }
    else {
      print "echo \"merging $line\"\n";
      print "$tools/mergeOneFile.sh $baselines/$old $baselines/$new $target $line\n";
      }
    next;
    }

  if ($line =~ m@Binary files $oldPat/(\S+) and .* differ@) {
    # updated binary distribution
    $line = $1;
    if (-l "$old/$line") {
      print "# link: $line\n";
      }
    else {
      print "echo \"copying $line\"\n";
      print "cp $baselines/$new/$line $target/$line\n";
      }
    next;
    }

  # created anew; copy over (recursively)
  if ($line =~ m@^Only in $newPat/(\S*):\s*(.*)@) {
    $dir = $1;
    $f = $2;
    if (-d "$baselines/$new/$dir/$f") {
      print "echo \"creating directory $dir/$f\"\n";
      &populate_new_dir("$baselines/$new/$dir", "$target/$dir", $f);
      }
    else {
      print "echo \"creating file $dir/$f\"\n";
      &create_new_file("$baselines/$new/$dir", "$target/$dir", $f);
      }
    next;
    }
  # only in old, delete
  if ($line =~ m@^Only in $oldPat/(\S*):\s*(\S+).*@) {
    $dir = $1;
    $f = $2;

    # This message can occur multiple times, we only want the first occurence.
    next if $removed_dirs{"$dir/$f"};
    $removed_dirs{"$dir/$f"} = 1;

    print "echo \"SKIP removing $dir/$f\"\n";
#     if (-d "$old/$dir/$f") {
#       print "echo \"removing directory $dir/$f\"\n";
#       print "pushd \$target/$dir\n";
#       print "cvs remove -f $f\n";;
#       print "popd\n";
# 
#       # directories don't remove very well under CVS.  This file
#       # is some extra help.  More work needed here....
#       open(F2, ">>$target/../fix.sh")
# 	|| die "unable to open $target/../fix.sh: $!";
#       print F2 "rm -rf jdk/$dir/$f\n";
#       close F2;
# 
#       # these changes must be quickly, but only when we commit the merge.
#       open(F2, ">>remove.sh") || die "unable to open remove.sh: $!";
#       print F2 "pushd jdk/$dir; pushd `l2repos`\n";
#       print F2 "mkdir Attic; chmod 777 Attic\n";
#       $junk = $f;
#       $junk =~ s#^([^/]*)/.*#$1#;
#       print F2 "mv $junk Attic\n";
#       print F2 "popd; popd\n";
#       close F2;
#       }
#     else {
#       print "echo \"removing file $dir/$f\"\n";
#       print "pushd \$target/$dir\n";
#       print "rm $f\n";
#       print "cvs remove $f\n";
#       print "popd\n";
#       }
    next;
    }
  }
close F;

sub populate_new_dir {
  local($oldDir, $newDir, $newName) = @_;
  local(@dirs, @files);

  # create the new directory
  print "pushd $newDir\n";
  print "mkdir $newName\n";
  print "cvs add $newName\n";
  print "popd\n";

  # create list of files and directories
  opendir(THISDIR, "$oldDir/$newName")
	|| die "opendir $oldDir/$newName failure: $!";
  for (;;) {
    $thisFile = readdir THISDIR;
    last if !defined($thisFile);
    next if $thisFile eq "." || $thisFile eq "..";
    next if -l "$oldDir/$newName/$thisFile";	# ignore links
    if (-d "$oldDir/$newName/$thisFile") {
      push(@dirs, $thisFile);
      }
    else {
      push(@files, $thisFile);
      }
    }
  closedir(THISDIR);

  foreach (sort(@files)) {
    &create_new_file("$oldDir/$newName", "$newDir/$newName", $_);
    }
  foreach (sort(@dirs)) {
    &populate_new_dir("$oldDir/$newName", "$newDir/$newName", $_);
    }
  }

sub create_new_file {
  local ($fromDir, $toDir, $newName) = @_;

  ($dev,$ino,$mode,@junk) = stat("$fromDir/$newName");
  print "cp $fromDir/$newName $toDir/$newName\n";
  print "pushd $toDir\n";
  print "chmod u+w $newName\n";
  if (-B "$fromDir/$newName") {
    print "cvs add -kb $newName\n";
    }
  else {
    print "cvs add $newName\n";
    }
  print "popd\n";
  }
