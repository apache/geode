# Apache Geode User Guide

This document contains instructions for building and viewing the Apache Geode User Guide locally and moving the built guide to the Apache Geode website for publication.

- [About](#about)
- [Prerequisites](#prerequisites)
- [Bookbinder Usage](#bookbinder-usage)
- [Building the Documentation](#building-the-documentation)
- [Embedding the User Guide in the Geode Website](#embedding-the-user-guide-in-the-geode-website)

## About

Apache Geode provides the full source for the Apache Geode User Guide in markdown format (see `{geode-project-dir}/geode-docs/CONTRIBUTE.md`). The latest check-ins to `{geode-project-dir}/geode-docs` on the `develop` branch are regularly built and published to http://geode.apache.org/docs/. Users can build the markdown into an HTML user guide using [Bookbinder](https://github.com/pivotal-cf/bookbinder) and the instructions below.

Bookbinder is a Ruby gem that binds  a unified documentation web application from markdown, html, and/or DITA source material. The source material for bookbinder must be stored either in local directories or in GitHub repositories. Bookbinder runs [Middleman](http://middlemanapp.com/) to produce a Rackup app that can be deployed locally or as a web application.

## Prerequisites

Bookbinder requires Ruby version 2.0.0-p195 or higher.

Follow the instructions below to install Bookbinder:

1. Add gem "bookbindery" to your Gemfile.
2. Run `bundle install` to install the dependencies specified in your Gemfile.

## Bookbinder Usage

Bookbinder is meant to be used from within a project called a **book**. The book includes a configuration file that describes which documentation repositories to use as source materials. Bookbinder provides a set of scripts to aggregate those repositories and publish them to various locations.

For Geode, a preconfigured **book** is provided in the directory `{geode-project-dir}/geode-book`, which gathers content from the directory `{geode-project-dir}/geode-docs`. You can use this configuration to build an HTML version of the Apache Geode User Guide on your local system.

## Building the Documentation

1. The GemFile in the `geode-book` directory already defines the `gem "bookbindery"` dependency. Make sure you are in the `{geode-project-dir}/geode-book` directory and enter:

    ```
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

   Note: You will not have to run `bundle install` on subsequent builds.

4. To start a local website of the Apache Geode User Guide, enter:

    ```
    $ rackup
    ```

   You can now view the local documentation at <http://localhost:9292>. 

## Publishing the User Guide to the Geode Website

Once you have reviewed your local build of the User Guide, you can publish it by copying it to the Apache Geode website. The target directory should contain a Geode version number. 

To copy the User Guide to the website repo:

1. Create the destination directory by navigating to the geode-site repo. Check out the *master* branch and create a destination directory for the User Guide. The naming convention is:

    ```
{geode-site}/website/content/docs/guide/XY
    ```
where `XY` is the product version of your documentation (e.g., `{geode-site}/website/content/docs/guide/12` if you are publishing the documentation for Apache Geode 1.2).

2. Navigate to the User Guide you have built in the Geode repository: `{geode-project-dir}/geode-book/final_app/public/docs/guide/XY`.

3. Use `tar` to copy the directory in order to preserve links and other filesystem niceties. Create the tarfile in your Desktop for easy access on the retrieval side.
  
  To copy the directory, enter:

    ```
    $ tar cvf ~/Desktop/new-guide-content.tar .
    $ cd {geode-site}/website/content/docs/guide/XY
    $ tar xvf ~/Desktop/new-guide-content.tar
    ```

4. Follow the instructions in the README.md file on the *master* branch of the geode-site repo (`{geode-site}/README.md`) to build, review, and publish the Apache Geode website. You can also view the geode-site README.md file on [github: https://github.com/apache/geode-site](https://github.com/apache/geode-site).
