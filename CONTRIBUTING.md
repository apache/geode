# Contributing
This document assumes you have followed the [Apache Geode Code contribution instructions](https://cwiki.apache.org/confluence/display/GEODE/Code+contributions)

## Building the code
    see BUILDING.md

## Next steps
* Make your changes/add your feature/fix a bug.
* Test your feature branch changes
* Check your formatting
* Submit a pull request

## Testing
   Before submitting a pull request the unit and integration tests must all pass. We are using CTest, (Please see [the CTest documentation](https://cmake.org/Wiki/CMake/Testing_With_CTest) for further information.)
### Running unit tests
    $ cd <clone>
    $ cd build

   The following steps will be updated once the "run-unit-tests" target is fixed.

    $ cd cppcache/test/<Debug|Release|if needed>
    $ ./gfcppcache_unittests
### Running integration tests
    $ cd <clone>
    $ cd build
    $ cmake --build . --target run-integration-tests

   Which is equivalent to running ctest directly:

    $ cd build/cppcache/integration-test
    $ ctest --timeout 2000 -L STABLE -C <Debug|Release> -R . -j1
   This will take ~ 2 hours, YMMV... you can up the jobs to 4 and run in parallel, but you may end up with test failures that will need to be re-run sequentially.  Like so:

    $ cd build/cppcache/integration-test
    $ ctest -R <test_name> -C <Debug|Release>

## Formatting C++
For C++ it is required to follow the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) and have a build target that uses [clang-format](https://clang.llvm.org/docs/ClangFormat.html) to achieve compliance.

    $ cd <clone>
    $ mkdir build
    $ cd build
    $ cmake ../src -DCLANG_FORMAT=<path to clang-format>
    $ cmake --build . --target format

# System Requirements

## All Platforms

### Required Tools
* [CMake 4.3](https://cmake.org/) or newer
* C++11 compiler *(see platform specific requirements)*
* [Doxygen 8.11](http://www.stack.nl/~dimitri/doxygen/download.html) *(for building source documentation)*

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
* [Solaris Studio 12.4](http://www.oracle.com/technetwork/server-storage/solarisstudio/downloads/index-jsp-141149.html) or newer

	