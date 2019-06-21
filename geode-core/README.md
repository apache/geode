# Geode Core Module

## JMH - Micro Benchmarks

### Running from Gradle
The JMH plugin appears to have issues with the latest Gradle daemon so disable it when running JMH
from Gradle. The following will run all JMH benchmarks.
```console
./gradlew --no-daemon jmh
```

#### Specific Benchmark
Add the `-Pjmh.includes=<benchmark>` project property to run specific benchmark.
```console
./gradlew --no-daemon jmh -Pjmh.include=<benchmark>
```

#### With JMH Profiler
Add the `-Pjmh.profilers=<profilers>` project property to run benchmarks with specific JMH profilers enabled.
```console
./gradlew --no-daemon jmh -Pjmh.include=<benchmark> -Pjmh.profilers=<profilers>
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
java -jar geode-core/build/libs/geode-core-*-SNAPSHOT-jmh.jar -l
```
Run with specific benchmark.
```console
java -jar geode-core/build/libs/geode-core-*-SNAPSHOT-jmh.jar <options> <benchmark>
```

#### With JMH Profiler
To get a list of available profilers.
```console
java -jar geode-core/build/libs/geode-core-*-SNAPSHOT-jmh.jar -lprof
```
Run with specific profiler.
```console
java -jar geode-core/build/libs/geode-core-*-SNAPSHOT-jmh.jar <options> -prof:<profiler> <benchmark>
```
