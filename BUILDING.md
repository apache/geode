# Building this Release from Source

Build instructions differ slightly for Unix and Windows platforms.
All platforms require a Java installation, with JDK 1.8 or more recent version.

## Build from Source on Unix

1. Set the JAVA\_HOME environment variable.  For example:

    ```     
    JAVA_HOME=/usr/java/jdk1.8.0_60
    export JAVA_HOME
    ```
2. Download the project source from the Releases page at [Apache Geode] (http://geode.apache.org), and unpack the source code.
3. Within the directory containing the unpacked source code, build without the tests:
    
    ```
    $ ./gradlew build -Dskip.tests=true
    ```
Or, build with the tests:
   
    ```
    $ ./gradlew build
    ```
The built binaries will be in `geode-assembly/build/distributions/`,
or the `gfsh` script can be found in 
`geode-assembly/build/install/apache-geode/bin/`.
4. Verify the installation by invoking `gfsh` to print version information and exit:
   
    ```
    $ gfsh version
    v1.0.0-incubating
    ```

## Build from Source on Windows

1. Set the JAVA\_HOME environment variable.  For example:

    ```
    $ set JAVA_HOME="C:\Program Files\Java\jdk1.8.0_60"
    ```
2. Install Gradle, version 2.12 or a more recent version.
3. Download the project source from the Releases page at [Apache Geode] (http://geode.apache.org), and unpack the source code.
4. Within the folder containing the unpacked source code, build without the tests:

    ```
    $ gradle build -Dskip.test=true
    ```
Or, build with the tests:

    ```
    $ gradle build
    ```
The built binaries will be in `geode-assembly\build\distributions\`,
or the `gfsh.bat` script can be found in 
`geode-assembly\build\install\apache-geode\bin\`.
4. Verify the installation by invoking `gfsh` to print version information and exit:
   
    ```
    $ gfsh.bat version
    v1.0.0-incubating
    ```


