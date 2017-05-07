#!/bin/bash -x
#This script is must be identical to vcvars_*_8.sh except for line below.
#This is done because you cannot get the path of a sourced script.
gf_arch_arg=64bit

function modern_vc_setup () {
  if [ -z "${VS10INSTALLDIR:-}" ]; then
    if [ -d "`cygpath 'c:\Program Files (x86)\Microsoft Visual Studio 10.0'`" ]; then
      export VS10INSTALLDIR=`cygpath -d 'c:\Program Files (x86)\Microsoft Visual Studio 10.0'`
    else
      echo "ERROR: Unable to determine Visual Studio version for env setup"
      exit -1
    fi  
  fi

  if [ -z "${MSSDK:-}" ]; then
    if [ -d "`cygpath 'C:\Program Files (x86)\Microsoft SDKs'`" ]; then
      export MSSDK=`cygpath -d 'C:\Program Files (x86)\Microsoft SDKs'`
    else
      echo "ERROR: Unable to determine Microsoft SDK path for env setup"
      exit -1
    fi  
  fi
  
  if [ "x$gf_arch_arg" == "x64bit" ]; then
    arch_bin="\\x86_amd64"
    arch_lib="\\amd64"
	x64_lib="\\x64"
  elif [ "x$gf_arch_arg" == "x32bit" ]; then
    arch_bin=""
    arch_lib=""
	x64_lib=""
  else
    echo "ERROR: Unable to determine Visual Studio version for env setup"
    exit -1
  fi
  # Compatible with Visual Studio 2010
  export VCINSTALLDIR="$VS10INSTALLDIR\VC"

  if [ -d "$VCINSTALLDIR" ]; then
    echo Setting environment for using Microsoft Visual Studio 2010 tools.
    export VCVER=vc10  
    export FrameworkDir="$SYSTEMROOT\\Microsoft.NET\\Framework"
    export FrameworkVersion=v4.0.30319
    export FrameworkSDKDir="$MSSDK\\Windows\\v7.0A"
    export DevEnvDir="$VS10INSTALLDIR\\Common7\\IDE"
  else
    echo "ERROR: Unable to determine Visual Studio version for env setup"
    exit -1
  fi

  VCPATH="$DevEnvDir;$VCINSTALLDIR\\BIN${arch_bin};$VCINSTALLDIR\\lib${arch_lib};$VS10INSTALLDIR\\Common7\\Tools;$VCINSTALLDIR\\Common7\\Tools\\bin;$FrameworkSDKDir\\bin;$FrameworkDir\\$FrameworkVersion"
  export PATH="`cygpath -up "$VCPATH"`:$PATH"
  export INCLUDE="$VCINSTALLDIR\\ATLMFC\\INCLUDE\;$VCINSTALLDIR\\INCLUDE\;$VCINSTALLDIR\\PlatformSDK\\include\;$FrameworkSDKDir\\include"
  export LIB="$VCINSTALLDIR\\ATLMFC\\LIB${arch_lib}\;$VCINSTALLDIR\\LIB${arch_lib}\;$FrameworkSDKDir\\lib${x64_lib}"
 echo PATH is $PATH
 echo lib is $LIB
 echo link.exe from `which link.exe`
}

modern_vc_setup
