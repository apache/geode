$ErrorActionPreference = "Stop"

choco install gnuwin --allowEmptyChecksums -confirm

# Fixup system path since installer puts it in user path
$env:Path += ";" + "C:\GnuWin\bin"

[Environment]::SetEnvironmentVariable( "Path", $env:Path, [System.EnvironmentVariableTarget]::Machine )
