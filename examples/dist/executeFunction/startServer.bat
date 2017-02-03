@echo off

rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem 
rem      http://www.apache.org/licenses/LICENSE-2.0
rem 
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

rem GFCPP must be set
rem GEMFIRE must be set


if not "%GEMFIRE%"=="" goto startexamples

echo GEMFIRE is not set.
goto finished


:startexamples


:runexample

echo.
echo Running GemFire Server

set CLASSPATH=%CLASSPATH%;../javaobject.jar;
set PATH=%GEMFIRE%\bin;%PATH%;%GEMFIRE%\bin;..\bin;

if not exist gfecs mkdir gfecs
if not exist gfecs2 mkdir gfecs2



call cacheserver start cache-xml-file=../XMLs/serverExecuteFunctions.xml mcast-port=35673 -dir=gfecs
call cacheserver start cache-xml-file=../XMLs/serverExecuteFunctions2.xml mcast-port=35673 -dir=gfecs2

rem pause

:finished
