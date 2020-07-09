# Using Docker to Build the Apache Geode User's Guide

This document contains instructions for building and viewing the Apache Geode User Guide locally.

Apache Geode provides the full source for the Apache Geode User Guide in markdown format (see `{geode-project-dir}/geode-docs/CONTRIBUTE.md`). For every Apache Geode release the user guide is built and published to http://geode.apache.org/docs/. Users can build the markdown into an HTML user guide by following the instructions below. The tooling builds upon a Docker image that includes [Bookbinder](https://github.com/pivotal-cf/bookbinder) and everything needed to run it. No local version of Ruby or bookbinder is needed.

The User Guide built in this way reflects any local updates you have made to the documentation source files in your local Apache Geode repository.

## Building the User Guide

To build the guide, open a shell, cd into this directoy and run

```bash
$ ./build-docs.sh
```

## Viewing the User Guide

After the HTML files are generated, `view-docs.sh` can be used to start a webserver and review the documentation.

```bash
$ ./view-docs.sh
```
In a browser, navigate to `http://localhost:9292` to view the user guide.

The other files in this folder (`build-image-common.sh` and `Dockerfile`) are utilities used by `build-docs.sh` and `view-docs.sh`.
