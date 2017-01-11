<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Contributing to the Apache Geode Documentation

Apache Geode welcomes your contributions to the community's documentation efforts. You can participate by writing new content, reviewing and editing existing content, or fixing bugs. This document covers the following topics:

- [Working with Markdown Files](#working-with-markdown-files)
- [Working with Images and Graphics](#working-with-images-and-graphics)
- [Writing Guidelines](#writing-guidelines)

For instructions on building the documentation locally, see `../geode-book/README.md`.

## Working with Markdown Files

You can edit markdown files in any text editor. For more, read [Daring Fireball's Markdown Syntax page](https://daringfireball.net/projects/markdown/syntax).

## Working with Images and Graphics

Image files in .gif or .png format are in the `docs/images` directory in the Apache Geode docs repo. Images in .svg format are in the `docs/images_svg` directory.

Most of the Apache Geode image files have been converted to the open source SVG format. You can insert SVG images directly into an XML topic and modify images using a SVG editor.

The Wikipedia page [Comparison of Vector Graphics Editors](http://en.wikipedia.org/wiki/Comparison_of_vector_graphics_editors) provides a list and comparison of commercial and free vector graphics editors. Note, however, that not all of these programs support the SVG format.

## Writing Guidelines

The most important advice we can provide for working with the Apache Geode docs is to spend some time becoming familiar with the existing source files and the structure of the project directory. In particular, note the following conventions and tips:

- Top-level subdirectories organize topics into "books": basic_config, configuring, developing, etc.
- Use lowercase characters for all file and directory names. Separate words in filenames with an underscore (`_`) character.
- Use the `.md` file extension for topic files.
- Add new topics to the existing directories by subject type. Only create a new directory if you are starting a new subject or a new book.
- To start a new topic, you can make a copy of an existing file with similar content and edit it.
- Use the appropriate document type for the content you are writing. Create multiple topics if you are writing overview, procedural, and reference content.
- To edit elements in the navigation pane (the "subnav") that appears on the left side of the documentation, navigate to `../geode-book/master_middleman/source/subnavs/geode-subnav.md`.
