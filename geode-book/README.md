# Apache Geode User Guide

This document contains instructions for building and viewing the Apache Geode User Guide locally and moving the built guide to the Apache Geode website for publication.

- [About](#about)
- [Automatic build](#automatic-build)
- [Manual build](#manual-build)
 - [Prerequisites](#prerequisites)
 - [Bookbinder Usage](#bookbinder-usage)
 - [Building the Documentation](#building-the-documentation)
- [Publishing the User Guide to the Geode Website](#publishing-the-user-guide-to-the-geode-website)

## About

Apache Geode provides the full source for the Apache Geode User Guide in markdown format (see `{geode-project-dir}/geode-docs/CONTRIBUTE.md`). For every Apache Geode release the user guide is built and published to http://geode.apache.org/docs/. Users can build the markdown into an HTML user guide using [Bookbinder](https://github.com/pivotal-cf/bookbinder) and the instructions below.

Bookbinder is a Ruby gem that binds  a unified documentation web application from markdown, html, and/or DITA source material. The source material for bookbinder must be stored either in local directories or in GitHub repositories. Bookbinder runs [Middleman](http://middlemanapp.com/) to produce a Rackup app that can be deployed locally or as a web application.

## Preview the User Guide

Documentation can be built and previewed using the utility script at `{geode-project-dir}/dev-tools/docker/docs`. This script uses a Docker image that provides the tools to build and view the guide, including Ruby, Bookbinder, and Rackup.

```bash
$ cd {geode-project-dir}/dev-tools/docker/docs
$ ./preview-user-guide.sh
```
In a browser, navigate to `http://localhost:9292` to view the user guide.

## Build the User Guide

### Prerequisites

Bookbinder requires Ruby version 2.0.0-p195 or higher.

Follow the instructions below to install Bookbinder:

1. Add gem "bookbindery" to your Gemfile.
2. Run `bundle install` to install the dependencies specified in your Gemfile.

### Bookbinder Usage

Bookbinder is meant to be used from within a project called a **book**. The book includes a configuration file that describes which documentation repositories to use as source materials. Bookbinder provides a set of scripts to aggregate those repositories and publish them to various locations.

For Geode, a preconfigured **book** is provided in the directory `{geode-project-dir}/geode-book`, which gathers content from the directory `{geode-project-dir}/geode-docs`. You can use this configuration to build an HTML version of the Apache Geode User Guide on your local system.

### Building the Documentation

1. The GemFile in the `geode-book` directory already defines the `gem "bookbindery"` dependency. Make sure you are in the `{geode-project-dir}/geode-book` directory and enter:

    ```
    $ cd {geode-project-dir}/geode-book
    $ bundle install
    ```

   Note: You will not have to run `bundle install` on subsequent builds.

2. To build the documentation locally using the installed `config.yml` file, enter:

    ```
    $ bundle exec bookbinder bind local
    ```

   Bookbinder converts the markdown source into HTML, which it puts in the `final_app` directory.

3. Navigate to `{geode-project-dir}/geode-book/final_app/` and enter:

    ```
    $ bundle install
    ```

   Note: You will not have to run `bundle install` on subsequent builds. If you see errors during this step regarding the inability to install libv8, try running the command:
    ```
    $ gem install libv8 -v '3.16.14.15' --  --with-system-v8
    ```
   This can help you avoid errors with the default GCC compiler provided by Xcode on MacOS. This will only help circumvent the use of GCC to install libv8, so if you're on
   an operating system where you do not have access to a system install of libv8 this may not be possible or necessary.


3. To start a local website of the Apache Geode User Guide, you have to execute Rackup from the `final_app` directory:

    ```
    $ cd {geode-project-dir}/geode-book/final_app
    $ bundle exec rackup
    ```

   You can now view the local documentation at <http://localhost:9292>.

## Publishing the User Guide to the Geode Website

Once you have reviewed your local build of the User Guide, you can publish it by copying it to the Apache Geode website. The target directory should contain a Geode version number.

To copy the User Guide to the website repo, follow the instructions in the `README.md` file on the *master* branch of the geode-site repo ([github: https://github.com/apache/geode-site](https://github.com/apache/geode-site)) under the topic **Add a new user guide**.
