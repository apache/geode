$ErrorActionPreference = "Stop"

Import-Module Packer

Install-Package https://download.microsoft.com/download/2/C/6/2C6E1B4A-EBE5-48A6-B225-2D2058A9CEFB/W2K12-KB3134759-x64.msu `
    -Hash 6E59CEC4BD30C505F426A319673A13C4A9AA8D8FF69FD0582BFA89F522F5FF00 `
    -MsuPackage .\Windows8-RT-KB3134759-x64.cab 