# Building
    $ cd <clone>
    $ mdkir build
    $ cmake ../src
    $ cmake --build .

# Platforms Known To Build
* [Mac OS X](#mac-os-x)
* [Windows](#windows)
* [Linux](#linux)
* [Solaris](#solaris)

# System Requirements
## All Platforms
### Required Tools
* [CMake 4.3](https://cmake.org/) or newer
* C++11 compiler *(see platform specific requirements)*

### Optional Tools
* [Doxygen 8.11](http://www.stack.nl/~dimitri/doxygen/download.html) *(If building source documentation)*

## Mac OS X
* Mac OS X 10.12 (Sierra)

### Required Tools
* [XCode](https://developer.apple.com/xcode/download/)
* Xcode command line developer tools

    `$ xcode-select --install` 

### Optional Tools
* [CMake GUI](https://cmake.org/files/v3.4/cmake-3.4.3-Darwin-x86_64.dmg)
* [Doxygen GUI](http://ftp.stack.nl/pub/users/dimitri/Doxygen-1.8.11.dmg)
* [Eclipse CDT 8.8](https://eclipse.org/cdt/) or newer

## Windows
* Windows 8.1 64-bit
* Windows 10 64-bit
* Windows Server 2012 64-bit
* Windows Server 2012 R2 64-bit

### Required Tools
* [Visual Studio 2013](https://www.visualstudio.com) or newer
* [Cygwin](https://www.cygwin.com/)

## Linux
* RHEL/CentOS 6
* RHEL/CentOS 7
* SLES 11
* SLES 12

### Required Tools
* [GCC 4.6](https://gcc.gnu.org) or newer

### Optional Tools
* [Eclipse CDT 8.8](https://eclipse.org/cdt/) or newer

## Solaris
* Solaris 10 SPARC
* Solaris 10 x86
* Solaris 11 SPARC
* Solaris 11 x86

### Required Tools
* [Solaris Studion 12.4](http://www.oracle.com/technetwork/server-storage/solarisstudio/downloads/index-jsp-141149.html) or newer

