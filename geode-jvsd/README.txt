#######################################################################################################################
####################################################	REQUIREMENTS 	###############################################
#######################################################################################################################
# Check Java JDK version, it should be 1.8.0_60 or later.
$java -version
java version "1.8.0_60"
Java(TM) SE Runtime Environment (build 1.8.0_60-b27)
Java HotSpot(TM) Server VM (build 25.60-b23, mixed mode)

# Check Maven version, it should be 3.2.3 or later (required by MultiAxisChart)
$mvn -version
Apache Maven 3.2.3 (33f8c3e1027c3ddde99d3cdebad2656a31e8fdf4; 2014-08-11T21:58:10+01:00)
Java version: 1.8.0_60, vendor: Oracle Corporation

# Clone Geode from the Apache repository and checkout JVSD branch. 
$git clone https://git-wip-us.apache.org/repos/asf/incubator-geode.git
$cd incubator-geode/
$git branch feature/GEODE-78 origin/feature/GEODE-78
$git checkout feature/GEODE-78

# Build and install the third party charting library, MultiAxisChartFX.
$./gradlew geode-jvsd:MultiAxisChart

#######################################################################################################################
####################################################		BUILD 		###############################################
#######################################################################################################################
# Build Geode.
$./gradlew clean build installDist -Dskip.tests=true

# Build Only jVSD.
$./gradlew geode-jvsd:clean geode-jvsd:build geode-jvsd:installDist -Dskip.tests=true

#######################################################################################################################
####################################################		RUN 		###############################################
#######################################################################################################################
# Run from source.
$./gradlew geode-jvsd:run

# Run from distribution.
$./geode-jvsd/build/install/geode-jvsd/bin/geode-jvsd
