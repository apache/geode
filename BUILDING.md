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

## Setting up IntelliJ

The following steps have been tested with:

* **IntelliJ IDEA 2018.2.2**

1. Run `./gradlew --parallel devBuild` from Geode repository root to create compiler generated source

2. Import project into IntelliJ IDEA

   From the **Welcome to IntelliJ IDEA** window:

   1. **Import Project ->** select *build.gradle* file from Geode repository root and press **Open**.
   2. Enable **Use auto-import**
   3. Enable **Create separate module per source set**
   4. Select **Use Project JDK 1.8.0_*nnn*** where *nnn* is latest build required for Geode

3. Change Code Style Scheme to GeodeStyle

   Navigate to **IntelliJ IDEA -> Preferences... -> Editor -> Code Style**. Select *GeodeStyle* in Scheme drop-down box if it already exists.

   To define the *GeodeStyle* in **Scheme**, select the gear icon next to the drop-down box, click **Import Scheme ->** and select **IntelliJ IDEA code style XML**. Select *etc/intellij-java-modified-google-style.xml* from Geode repository root, enter **To:** *GeodeStyle*, check **Current scheme** and press **OK**.

4. Make Apache the default Copyright

   Navigate to **IntelliJ IDEA -> Preferences... -> Editor -> Copyright**. Select *Apache* in drop-down box **Default project copyright**.

   To define *Apache* in **Copyright**, navigate to **IntelliJ IDEA -> Preferences... -> Editor -> Copyright -> Copyright Profiles**. Click **+** to add a new project. Enter *Apache* as the **Name** and enter the following block without asterisks or leading spaces:

   ```text
   Licensed to the Apache Software Foundation (ASF) under one or more contributor license
   agreements. See the NOTICE file distributed with this work for additional information regarding
   copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance with the License. You may obtain a
   copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software distributed under the License
   is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
   or implied. See the License for the specific language governing permissions and limitations under
   the License.
   ```

   ...then return to **Copyright** and select *Apache* in drop-down box **Default project copyright**.

   Navigate to **IntelliJ IDEA -> Preferences... -> Editor -> Copyright -> Formatting**. Uncheck **Add blank line after** and press **OK**.

5. Rebuild Project

   Navigate to **Build -> Rebuild Project** and the full project should compile without errors.

   Some optional sanity tests to make sure things are working properly:
   * Try looking up classes using **Navigate -> Class...**
   * Open and run a distributed test such as BasicDistributedTest in geode-core.
   * Create a new java class and ensure the Apache license is automatically added to the top of the file with no blank line before the package line.
