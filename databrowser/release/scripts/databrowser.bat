@setlocal enableextensions
@set scriptdir=%~dp0
@set databrowser=%scriptdir:\bin\=%
@if exist "%databrowser%\lib\__JAR_NAME__" @goto databrowserok
@echo Could not determine the DataBrowser's location
@verify other 2>nul
@goto done

:databrowserok
@if defined GEMFIRE @goto gemfirevariableok 
@echo ERROR: GEMFIRE environment variable is not set.
@verify other 2>nul
@goto done
:gemfirevariableok

@if exist "%GEMFIRE%/lib/gemfire.jar" @goto gemfireok
@echo Could not determine locate gemfire.jar, expected it at %%GEMFIRE%%\lib\gemfire.jar
@verify other 2>nul
@goto done
:gemfireok

@if not defined GF_JAVA (
@REM %GF_JAVA% is not defined, assume it is on the PATH
@set GF_JAVA=java
)

@set TOOL_JARS=%databrowser%\lib\__JAR_NAME__
@echo off

@rem Detect JVM Data/Architecture Model
for /f "tokens=*" %%a in (
'@"%GF_JAVA%" -classpath "%TOOL_JARS%" "-Duser.language=us" __JVMARCH_CLASS__'
) do (
@set GF_JAVA_ARCH=%%a
)
@set ARCH_JAR=__SWT_x86.Windows_NT__
@if %GF_JAVA_ARCH%==x86_64 (
@set ARCH_JAR=__SWT_x86_64.Windows_NT__
)

@rem try "mkdir" for the temp directory that will be used by SWT for native libraries
@set TEMP_DIR=%scriptdir%\temp
@set TEMP_DIR_ARCH=%TEMP_DIR%\Win-%GF_JAVA_ARCH%
mkdir %TEMP_DIR_ARCH% 2>nul

@if exist "%TEMP_DIR_ARCH%" @goto tempdirok
@echo ERROR: Could not find/create the required temporary directory %TEMP_DIR_ARCH%. Please check for write permissions in %scriptdir%.
@verify other 2>nul
@goto done
:tempdirok

@set MX4J_JARS=%GEMFIRE%/lib/commons-logging.jar;%GEMFIRE%/lib/commons-modeler-2.0.jar;%GEMFIRE%/lib/mx4j.jar;%GEMFIRE%/lib/mx4j-remote.jar;%GEMFIRE%/lib/mx4j-tools.jar
@set TOOL_JARS=%TOOL_JARS%;%GEMFIRE%\lib\gemfire.jar;%GEMFIRE%\lib\antlr.jar;%MX4J_JARS%;%databrowser%\lib\__Windows_NT_JARS__;%databrowser%\lib\%ARCH_JAR%
@if defined CLASSPATH (
@set TOOL_JARS=%TOOL_JARS%;%CLASSPATH%
)
@"%GF_JAVA%" %JAVA_ARGS% -classpath "%TOOL_JARS%" -Duser.language=us -Djava.io.tmpdir="%TEMP_DIR_ARCH%" __MAIN_CLASS__ %*

@rem Try deleting parent temp directory so that libraries remain for other instances
rd /S /Q %TEMP_DIR% 2>nul
:done
@rem prevent subsequent invocations from appending repeatedly
@set TOOL_JARS=
@set MX4J_JARS=
@set TEMP_DIR=
@set TEMP_DIR_ARCH=
