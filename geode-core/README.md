# Geode Core Module

## JMH - Micro Benchmarks

### Running from Gradle
The JMH plugin appears to have issues with the latest Gradle daemon so disable it when running JMH
from Gradle. The following will run all JMH benchmarks.
```console
./gradlew jmh
```

#### Specific Benchmark
Add the `-Pjmh.includes=<benchmark>` project property to run a specific benchmark.
```console
./gradlew jmh -Pjmh.include=<benchmark>
```

#### With JMH Profiler
Add the `-Pjmh.profilers=<profilers>` project property to run benchmarks with specific JMH profilers enabled.
```console
./gradlew jmh -Pjmh.include=<benchmark> -Pjmh.profilers=<profilers>
```

#### All JMH options
Specify all options as `-Pjmh.<option>=<value>`. All options are defined by the
[JMH Gradle Plugin](https://github.com/melix/jmh-gradle-plugin#configuration-options). Not all
options have been passed through. You can add missing option in the jmh.gradle file.
```console
./gradlew jmh -Pjmh.include=<benchmark> -Pjmh.jvm=/path/to/java11/bin/java
```

### Running with JMH Uber Jar
#### Building
You can build an uber jar containing all the built JMF benchmarks and runtime dependencies.
```console
./gradlew jmhJar
```

### Running
Due to vesion mismatches on jopt you cannot get `-h` help output from JMH. Please see 
[JMH Tutorial](https://github.com/guozheng/jmh-tutorial/blob/master/README.md) for command line
options.

#### Specific Benchmark
To list benchmarks.
```console
java -jar geode-core/build/libs/geode-core-*-jmh.jar -l
```
Run with specific benchmark.
```console
java -jar geode-core/build/libs/geode-core-*-jmh.jar <options> <benchmark>
```

#### With JMH Profiler
To get a list of available profilers.
```console
java -jar geode-core/build/libs/geode-core-*-jmh.jar -lprof
```
Run with specific profiler.
```console
java -jar geode-core/build/libs/geode-core-*-jmh.jar <options> -prof:<profiler> <benchmark>
```
