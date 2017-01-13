Set-PSDebug -Trace 0

function Install-Package {
  [CmdletBinding()]
  param (
    [parameter(Mandatory=$false,Position=0)]
    [string]$Uri,
    
    [parameter(Mandatory=$false)]
    [string]$Installer,
    
    [parameter(Mandatory=$false)]
    [string]$Log,

    [parameter(Mandatory=$false)]
    [string[]]$ArgumentList=@(),

    [parameter(Mandatory=$false)]
    [string]$Hash,

    [parameter(Mandatory=$false)]
    [string]$DestinationPath,

    [parameter(Mandatory=$false)]
    [System.Collections.IDictionary]$DownloadHeaders=@{},

    [parameter(Mandatory=$false)]
    [string]$DownloadMethod="Get",

    [parameter(Mandatory=$false)]
    [string]$MsuPackage
  )
  PROCESS {
    Push-Location -Path $Env:temp
  
    if (-not $Installer) {
      $Installer = $Env:temp + "\" + $Uri.Split('/')[-1]
    }

    Write-Verbose "Install-Package: Uri=$Uri, Installer=$Installer, ArgumentList=$ArgumentList Log=$Log"
    
    if ($Uri) {

    if (!((Test-Path $Installer) -and ((Get-FileHash $Installer).hash -eq "$Hash"))) {

        if ($DownloadMethod -eq 'GET') {
        $ds = New-Object psobject -Property @{downloadProgress = 0; downloadComplete = $false; error = 0}
      
        $wc = New-Object System.Net.WebClient
      
        if ($DownloadHeaders.Count -gt 0) {
          $a = $DownloadHeaders.GetEnumerator() | % { "$($_.Name):$($_.Value)" }
          $wc.Headers.Add($a)
        }
      
        $eventDataComplete = Register-ObjectEvent $wc DownloadFileCompleted `
            -MessageData $ds `
            -Action { 
                $event.MessageData.downloadComplete = $true
                $event.MessageData.error = $EventArgs.Error
            }
      
        $eventDataProgress = Register-ObjectEvent $wc DownloadProgressChanged `
            -MessageData $ds `
            -Action {
                $event.MessageData.downloadProgress = $EventArgs.ProgressPercentage
            }    
      
        while ($true) {
        $ds.error = 0
        $ds.downloadComplete = $false
        $ds.downloadProgress = 0
        
        try {
            $wc.DownloadFileAsync($Uri, $Installer)
        } catch {
            Write-Host $_.Exception.Message
        }
      
      
        $p = 0;
        while (!$ds.downloadComplete) {
        if ($ds.downloadProgress -gt $p) {
            $p = $ds.downloadProgress;
            Write-Host "Downloading... ($($ds.downloadProgress)%)"
            Start-Sleep -m 100
        }
        }
        if ($ds.error) {
        Write-Host "Error: $($ds.error)"
        } else {
        break;
        }
        }
      } else {
        # POST
        Invoke-WebRequest -Uri $Uri `
                        -Headers $DownloadHeaders `
                        -Method $DownloadMethod `
                        -OutFile $Installer
      }
    }
    }

    Write-Host "Installing..."
    if ($Installer -match "\.msi$") {
      Write-Verbose "Installing via MSI"
      $Log = "$Installer.log"
      $ArgumentList = @("/package", $Installer, "/quiet", "/log", "$Log") + $ArgumentList
      $Installer = "msiexec";
    } elseif ($Installer -match "\.msu$") {
      Write-Verbose "Installing via MSU"
      $Log = "$Installer.log"
      Start-Process -FilePath "wusa" -ArgumentList @($Installer, "/extract:.")
      $ArgumentList = @("/Online", "/Add-Package", "/NoRestart", "/PackagePath:$MsuPackage") + $ArgumentList
      $Installer = "dism";
    } elseif ($Installer -match "\.zip$") {
      Write-Verbose "Installing via ZIP"
      Expand-Archive -Path $Installer -DestinationPath $DestinationPath -Force -Verbose
      $Installer = "";
    }

    if ($Installer) {
    Write-Verbose "Installer=$Installer, ArgumentList=$ArgumentList"
    $ip = Start-Process -FilePath $Installer -ArgumentList $ArgumentList -NoNewWindow -PassThru
    if (!$ip) {
      throw "Error starting installer. Installer=$Installer, ArgumentList=$ArgumentList"
    }
    $handle = $ip.Handle

    if ($log) {
      $lp = Start-Process -FilePath powershell.exe -ArgumentList @("-Command", "& {Import-Module Packer; Get-Tail -FilePath $Log -Follow}") -NoNewWindow -PassThru
      #$lp= &{ Tail-File $Log -Follow }
      #$lp
    }

    while(-not $ip.HasExited) {
      Write-Host -NoNewline '.'
#      if ($Log) {
#        $c = Get-Content -Path $Log -Tail 1
#        Write-Host ">> $c"
#      }
      sleep 1
    }
    
    $lp | Stop-Process -ErrorAction SilentlyContinue

    Write-Verbose "Exit Code: $($ip.ExitCode)"
    if ($ip.ExitCode -eq 0) {
      Write-Host "Installation complete."
    } elseif ($ip.ExitCode -eq 3010) {
      Write-Host "Restart required to complete installation."
    } else {
      throw "Error while installing. Installer exit code $($ip.ExitCode). Installer=$Installer, ArgumentList=$ArgumentList"
    }
    }
    
    Pop-Location
  }
}

function Get-Tail { 
 [CmdletBinding()]
  param (
    [parameter(Mandatory=$true,Position=0)]
    [string]$FilePath,

    [parameter(Mandatory=$false)]
    [int]$Offset,
    
    [parameter(Mandatory=$false)]
    [switch]$Follow
  )
  PROCESS {
    Write-Verbose "Tail-File: FilePath=$FilePath, Follow=$Follow"
    
    while (1) {
      $ci = get-childitem $FilePath -ErrorAction SilentlyContinue
      if ($ci) { break }
      Start-Sleep -m 100
    }
    
    $fullName = $ci.FullName
    
    $reader = new-object System.IO.StreamReader(New-Object IO.FileStream($fullName, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [IO.FileShare]::ReadWrite))
    #start at the end of the file
    $lastMaxOffset = $reader.BaseStream.Length - $Offset

    while ($true)
    {
      $reader = new-object System.IO.StreamReader(New-Object IO.FileStream($fullName, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [IO.FileShare]::ReadWrite))
      #if the file size has not changed, idle
      if ($reader.BaseStream.Length -ge $lastMaxOffset) {
        #seek to the last max offset
        $reader.BaseStream.Seek($lastMaxOffset, [System.IO.SeekOrigin]::Begin) | out-null

        #read out of the file until the EOF
        $line = ""
        while (($line = $reader.ReadLine()) -ne $null) {
            write-output $line
        }

        #update the last max offset
        $lastMaxOffset = $reader.BaseStream.Position
      } elseif ($reader.BaseStream.Length -lt $lastMaxOffset) {
        write-output "File truncated"
        $lastMaxOffset = 0;
      }
  
      if($Follow){
          Start-Sleep -m 100
      } else {
          break;
      }
    }
    
  }
}