$vs = "Microsoft Visual Studio 12.0"
$arch = $args[0]

pushd "c:\Program Files (x86)\$vs\VC\"
cmd /c "vcvarsall.bat $arch & set" |
foreach {
  if ($_ -match "=") {
    $v = $_.split("="); set-item -force -path "ENV:\$($v[0])"  -value "$($v[1])"
  }
}
popd
write-host "Environment setup for $vs $arch." -ForegroundColor Yellow
