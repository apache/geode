$ErrorActionPreference = "Stop"
Import-Module Packer -Force

mkdir C:\gemfire
cd C:\gemfire
cmake -E tar zxf $Home\gemfire.tar.gz
rm $Home\gemfire.tar.gz
