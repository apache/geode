$ErrorActionPreference = "Stop"
Import-Module Packer

Install-Package http://downloads.activestate.com/ActivePerl/releases/5.22.3.2204/ActivePerl-5.22.3.2204-MSWin32-x64-401627.exe `
    -Hash 214083166032FBF61F20BB4C65B0610508B90101910F3293B67BD56619E6444D `
    -ArgumentList @("/qn", "/norestart", "PERL_PATH=Yes", "PERL_EXT=Yes")
