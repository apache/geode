$ErrorActionPreference = "Stop"
Import-Module Packer

Install-Package http://downloads.activestate.com/ActivePerl/releases/5.22.1.2201/ActivePerl-5.22.1.2201-MSWin32-x64-299574.msi `
    -Hash 214083166032FBF61F20BB4C65B0610508B90101910F3293B67BD56619E6444D `
    -ArgumentList @("PERL_PATH=Yes", "PERL_EXT=Yes")
