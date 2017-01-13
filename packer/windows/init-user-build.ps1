$ssh = "C:\Users\build\.ssh"
$authorized_keys = "$ssh\authorized_keys"
if ( -not (Test-Path $authorized_keys -PathType Leaf) ) {

  write-host "Installing SSH authorized key"

  mkdir -p $ssh -ErrorAction SilentlyContinue

  Invoke-WebRequest -Uri 'http://169.254.169.254/latest/meta-data/public-keys/0/openssh-key' -OutFile $authorized_keys
  
  Stop-Service sshd
  Start-Service sshd
}

