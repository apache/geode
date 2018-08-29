# Building this Release from Source

All platforms require a Java installation, with JDK 1.8 or more recent version.

Set the JAVA\_HOME environment variable.  For example:

| Platform | Command |
| :---: | --- |
|  Unix    | ``export JAVA_HOME=/usr/java/jdk1.8.0_121``            |
|  OSX     | ``export JAVA_HOME=`/usr/libexec/java_home -v 1.8``    |
|  Windows | ``set JAVA_HOME="C:\Program Files\Java\jdk1.8.0_121"`` |

Download the project source from the Releases page at
[Apache Geode](http://geode.apache.org/releases/), and unpack the source code.

Within the directory containing the unpacked source code, run the gradle build:
```console
$ ./gradlew build
```

Once the build completes, the project files will be installed at
`geode-assembly/build/install/apache-geode`. The distribution archives will be
created in `geode-assembly/build/distributions/`.

Verify the installation by invoking the `gfsh` shell command to print version
information:
```console
$ ./geode-assembly/build/install/apache-geode/bin/gfsh version
v1.1.0
```

Note: on Windows invoke the `gfsh.bat` script to print the version string.
