#! /bin/csh

source mergesetup.csh

pushd $baselines
gdiff -wBrc $old $new > $gdiffLog
if ( $status > 1) then
  echo "gdiff failed";
  exit 1
endif
echo "gdiff ok"
popd

perl scanHs.pl > $mergeScript
if ( $status != 0) then
 echo "perl scanHs.pl failed"
 exit 1
endif
echo "perl scanHs.pl ok"

