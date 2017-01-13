$ErrorActionPreference = "Stop"

# Chocolatey broken
#choco install cygwin cyg-get -confirm
#cyg-get git make
#exit 0

Import-Module Packer

Install-Package https://cygwin.com/setup-x86_64.exe `
    -Hash 08079A13888B74F6466DEF307A687E02CB26FC257EA2FA78D40F02E28330FD56 `
    -ArgumentList @(
        "--quiet-mode",
        "--verbose",
        "--site", "http://cygwin.osuosl.org", 
        "--packages", "git,patch,make"
        )

# Set Cygwin home to Windows profile
Add-Content C:\cygwin64\etc\nsswitch.conf "db_home: /%H"

# Remove Perl to avoid conflicts with AvtivePerl
Remove-Item -Path C:\cygwin64\bin\perl.exe
