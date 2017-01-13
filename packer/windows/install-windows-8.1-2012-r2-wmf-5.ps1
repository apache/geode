$ErrorActionPreference = "Stop"

Import-Module Packer

Install-Package https://download.microsoft.com/download/2/C/6/2C6E1B4A-EBE5-48A6-B225-2D2058A9CEFB/Win8.1AndW2K12R2-KB3134758-x64.msu `
    -MsuPackage .\WindowsBlue-KB3134758-x64.cab