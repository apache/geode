Check Java JDK version, it should version 1.8.0_60 or later.

$java --version
java version "1.8.0_60"
Java(TM) SE Runtime Environment (build 1.8.0_60-b27)
Java HotSpot(TM) Server VM (build 25.60-b23, mixed mode)

$jps
13760 Jps
 
Clone Geode from the Apache repository, checkout JVSD branch and then build Geode. 
 
$git clone https://git-wip-us.apache.org/repos/asf/incubator-geode.git
$cd incubator-geode/
$git branch jvsd origin/feature/GEODE-78
$git checkout jvsd
$./gradlew clean build installDist -Dskip.tests=true

Clone and build the third party charting library MultiAxisChartFX.
$cd ..
$git clone https://github.com/gemfire/MultiAxisChartFX
$cd MultiAxisChartFX/
$mvn install

Install the MultiAxisChart jar into local maven repository.

$mvn install:install-file \
-Dfile=./MultiAxisChart-1.0-SNAPSHOT.jar \
-DgroupId=com.pivotal.javafx \
-DartifactId=MultiAxisChart \
-Dversion=1.0-SNAPSHOT \
-Dpackaging=jar \
-DgeneratePom=true

For testing, additional jars may need to be added from the Apache Geode build to
the local Maven repository. 

mvn install:install-file -Dfile=gemfire-core-1.0.0-incubating-SNAPSHOT.jar \
-DgroupId=org.apache.geode \
-DartifactId=gemfire-core \
-Dversion=1.0.0-incubating-SNAPSHOT \
-Dpackaging=jar \
-DgeneratePom=true

mvn install:install-file -Dfile=gemfire-jgroups-1.0.0-incubating-SNAPSHOT.jar \
-DgroupId=org.apache.geode \
-DartifactId=gemfire-jgroups \
-Dversion=1.0.0-incubating-SNAPSHOT \
-Dpackaging=jar \
-DgeneratePom=true

mvn install:install-file -Dfile=fastutil-7.0.2.jar \
-DgroupId=org.apache.geode \
-DartifactId=fastutil \
-Dversion=7.0.2 \
-Dpackaging=jar \
-DgeneratePom=true

Change back into the Geode directory and then into the JVSD directory. Build JVSD.
$cd ../incubator-geode
$cd jvsdfx-mm/
$mvn install

Run the JVSD application.
$runjvsd.sh



