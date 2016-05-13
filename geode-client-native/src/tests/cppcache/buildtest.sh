#!/bin/bash

testname=$1
builddir=../..
artifacts="${builddir}/build-artifacts/win"

cl \
/Zi \
/nologo \
/GR \
/GX \
/DWIN32 \
/D_WIN32 \
/DWINVER=0x0500 \
/D_WIN32_WINNT=0x0500 \
/DIAL \
/D_X86_ \
/Dx86 \
/W3 \
'-DGEMFIRE_VERSION=""' '-DGEMFIRE_BUILDID=""' '-DGEMFIRE_BUILDDATE=""' '-DGEMFIRE_BUILDOS=""' \
/D__GEMFIRE_USE_STL_STRING \
/MDd \
/Od \
/DDEBUG \
/I"${artifacts}/product/include" \
/I"${builddir}/src/com/gemstone/gemfire/internal/cppcache" \
/I"${builddir}/src/com/gemstone/gemfire/internal/cppcache/impl" \
/I"${artifacts}/tests/cppcache" \
/I'//n080-fil01/java/users/java_share/ace5.4/x86.Windows_NT' \
/D__ACE_INLINE__ \
/Fe"${artifacts}/tests/cppcache/${testname}.exe" \
/Fd"${artifacts}/tests/cppcache/${testname}.pdb" \
${testname}.cpp \
\
/link  /LIBPATH:"${artifacts}/product/lib"  /LIBPATH:"${artifacts}/hidden/lib" gemfire_g.lib gfcppcache_g.lib ACE.lib
