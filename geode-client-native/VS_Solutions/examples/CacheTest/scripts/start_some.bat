REM
REM get the party started
REM

SET CACHETEST_PATH=D:\work\gemstone\gfcpp\trunk\VS_Solutions\examples\CacheTest\Debug

SET XML_FILE=%CACHETEST_PATH\..\xml\

SET CMD_ARGS=--burstct=4 --burstus=10000 --task=put

start "01_cache.xml" %CACHETEST_PATH%\CacheTest.exe %CMD_ARGS% --xml=%XML_FILE%\01_cache.xml
start "02_cache.xml" %CACHETEST_PATH%\CacheTest.exe %CMD_ARGS% --xml=%XML_FILE%\02_cache.xml
REM start "03_cache.xml" %CACHETEST_PATH%\CacheTest.exe %CMD_ARGS% --xml=%XML_FILE%\03_cache.xml
REM start "04_cache.xml" %CACHETEST_PATH%\CacheTest.exe %CMD_ARGS% --xml=%XML_FILE%\04_cache.xml
start "ct server 01" %CACHETEST_PATH%\CacheTest.exe %CMD_ARGS% --server=2
REM start "ct server 02" %CACHETEST_PATH%\CacheTest.exe %CMD_ARGS% --server=2
exit