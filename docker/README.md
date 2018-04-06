# Building the container image

The current Dockerfile is based on the [OpenJDK image](https://hub.docker.com/_/openjdk/) and includes the officially released Apache Geode binaries which are verified via GPG _and_ SHA256.

```
docker build .
```

If you're updating the image for a release, tag the build with the version:

```
docker build -t apachegeode/geode:{version} .
docker build -t apachegeode/geode:latest .
```

Once it's tagged, push to DockerHub:

```
docker push apachegeode/geode:{version}
```

* You need to be authenticated in DockerHub and be an administrator of the project.  Ask for permissions at *dev@geode.apache.org*.
* This may take a while depending on your internet connection.

# Starting a locator and gfsh

1. Execute the following command to run the container and start `gfsh`:

```
docker run -it -p 10334:10334 -p 7575:7575 -p 1099:1099  apachegeode/geode
```

From this point you can pretty much follow [Apache Geode in 5 minutes](https://cwiki.apache.org/confluence/display/GEODE/Index#Index-Geodein5minutes) for example:

```
gfsh> start locator --name=locator
gfsh> start server --name=server
```

But in order to have real fun with containers you are probably better off using something like docker-compose, Cloud foundry or Kubernetes. Those examples will come next.
