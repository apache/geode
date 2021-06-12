# Building this Release from Source

All platforms require a Java installation, with JDK 1.8 or more recent version.

Set the JAVA\_HOME environment variable. For example:

| Platform | Command |
| :---: | --- |
|  Unix    | ``export JAVA_HOME=/usr/java/jdk1.8.0_121``            |
|  OSX     | ``export JAVA_HOME=`/usr/libexec/java_home -v 1.8` ``  |
|  Windows | ``set JAVA_HOME="C:\Program Files\Java\jdk1.8.0_121"`` |

Download the project source from the Releases page at
[Apache Geode](http://geode.apache.org/releases/), and unpack the source code.

Within the directory containing the unpacked source code, run the gradle build:

```console
$ ./gradlew build
```

Once the build completes, the project files will be installed at
`geode-assembly/build/install/apache-geode`. The distribution archives will be created
in `geode-assembly/build/distributions/`.

Verify the installation by invoking the `gfsh` shell command to print version information:

```console
$ ./geode-assembly/build/install/apache-geode/bin/gfsh version
v1.1.0
```

Note: on Windows invoke the `gfsh.bat` script to print the version string.

## Setting up IntelliJ

The following steps have been tested with **IntelliJ IDEA 2020.3.3**

1. Run `./gradlew --parallel generate` from Geode repository root to create compiler generated
   source.

1. Import the project into IntelliJ IDEA.

    1. Select  **File -> Open...** from the menu.
    1. Select the `build.gradle` file in the Geode repository root and select **Open**.
    1. In the **Open Project?** popup, select **Open Project**.
    1. In the **Trust and Open Gradle Project?** popup, select **Trust Project**.
    1. Wait for IntelliJ to import the project and complete its background tasks.

1. Configure IntelliJ IDEA to build and run the project and tests.
    * Set the Java SDK for the project.
        1. Select **File -> Project Structure...** from the menu.
        1. Open the **Project Settings -> Project** section.
        1. Set **Project SDK** to your most recent Java 1.8 JDK.

    * To automatically re-generate sources when needed (recommended).
        1. Select **View -> Tool Windows -> Gradle** from the menu.
        1. In the Gradle dockable, open **geode -> Tasks -> build**.
        1. Right click the **generate** task and select **Execute Before Sync**.
        1. Right click the **generate** task and select **Execute Before Build**.

    * To reload the project when build scripts change (recommended).
        1. Select **IntelliJ IDEA -> Preferences...** from the menu.
        1. Open the **Build, Execution, Deployment -> Build Tools** section.
        1. Set **Reload project after changes in the build scripts:** to **Any changes**.

    * To build and run with Gradle (recommended).
        1. Select **IntelliJ IDEA -> Preferences...** from the menu.
        1. Open the **Build, Execution, Deployment -> Build Tools -> Gradle** section.
        1. Set **Build and run using:** to **Gradle**.
        1. Set **Run tests using:** to **Gradle**.

1. Set the Code Style Scheme to GeodeStyle.

    1. Select **IntelliJ IDEA -> Preferences...**
    1. Open the **Editor -> Code Style** section.
    1. If *GeodeStyle* style does not appear in the **Scheme** drop-down box
        1. Select the gear icon next to the drop-down.
        1. Select **Import Scheme -> IntelliJ IDEA code style XML**.
        1. Select `etc/intellij-java-modified-google-style.xml` from the Geode repository root.
        1. Enter **To:** *GeodeStyle*, check **Current scheme**, and press **OK**.
    1. Select *GeodeStyle* in **Scheme** drop-down box.

1. Make Apache the default Copyright.

    1. Select **IntelliJ IDEA -> Preferences...** from the menu.
    1. Open the **Editor -> Copyright** section.
    1. If *Apache* does not appear in the **Default project copyright** drop-down box:
        1. Open the **Copyright Profiles** subsection.
        1. Select the "import" icon (the small arrow pointing down and to the left) from the
           Copyright Profiles section's toolbar.
        1. Select `etc/intellij-apache-copyright-notice.xml` from the Geode repository root.
        1. Return to the **Copyright** section.
    1. Select *Apache* in the **Default project copyright** drop-down box.
    1. Open the **Formatting** subsection.
    1. Uncheck **Add blank line after** and select **OK**.

1. Rebuild the Project.

    1. Select **Build -> Rebuild Project** from the menu. The full project should compile without
       errors.

   Some optional sanity tests to make sure things are working properly:
    * Try looking up classes using **Navigate -> Class...**
    * Open and run a distributed test such as BasicDistributedTest in geode-core.
    * Create a new java class and ensure the Apache license is automatically added to the top of the
      file with no blank line before the package line.
