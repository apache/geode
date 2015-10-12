# Building the container image

The current Dockerfile is based on a CentOS 6 image, downloads JDK 8, clone the Apache Geode git repository, starts a build and execute the basic tests.

```
docker build -t apachegeode/geode:unstable .
```

This may take a while depending on your internet connection, but it's worth since this is a one time step and you endup with a container that is tested and ready to be used for development. It will download Gradle and as part of the build, project dependencies as well.

# Starting a locator and gfsh

1. Then you can start gfsh as well in order to perform more commands:

```
docker run -it -p 10334:10334 -p 7575:7575 -p 1099:1099  apachegeode/geode:unstable gfsh
```


From this point you can pretty much follow [Apache Geode in 5 minutes](https://cwiki.apache.org/confluence/display/GEODE/Index#Index-Geodein5minutes) for example:

```
start server --name=server1
```

But in order to have real fun with containers you are probably better off using something like docker-compose or kubernetes. Those examples will come next.

# Creating a cluster using multiple containers

Install docker-compose following the instructions on this [link](https://docs.docker.com/compose/install/) and move into the composer directory.

There is a docker-compose.yml example file there with a locator and a server.  To start the cluster execute:

```
docker-compose up
```

Or in order to start it in background:

```
docker-compose up -d
```

Do a docker ps and identify the container ID for the locator.  Now you can use *gfsh* on this container and connect to the distributed system:

```
docker exec -it <locator_container_id> gfsh
gfsh>connect --locator=locator[10334]
Connecting to Locator at [host=locator, port=10334] ..
Connecting to Manager at [host=192.168.99.100, port=1099] ..
Successfully connected to: [host=192.168.99.100, port=1099]

gfsh>list members
    Name     | Id
    ------------ | --------------------------------------
    locator      | locator(locator:33:locator)<v0>:1351
    6e96cc0f6b72 | 172.17.1.92(6e96cc0f6b72:34)<v1>:28140
```

Type exit and now to scale the cluster you can leverage docker-compose scale command. For example:

```
docker-compose scale server=3
```

This will start 2 extra Geode server containers. You can verify this step by repeating the last GFSH step and listing the members.

