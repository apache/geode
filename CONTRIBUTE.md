Contributing to the Project Geode Documentation
===============================================

Project Geode welcomes your contributions to our documentation efforts. You can participate by writing new content, editing existing content, fixing bugs, and reviewing content. This document covers the following topics:

-   [How to Contribute](#contribute)
-   [Document Source Files and Tools](#source_tools)
-   [Writing Guidelines](#guidelines)

<a name="contribute"></a>
## How to Contribute

We use the "fork and pull" collaboration method on GitHub:

1.  In your GitHub account, fork the Project-Geode/docs repository.
2.  Create a local clone of your fork.
3.  Make changes and commit them in your fork.
4.  In the Project-Geode/docs repository, create a pull request.

See [Using Pull Requests](https://help.github.com/articles/using-pull-requests/) on GitHub for more about the fork and pull collaboration method.


<a name="source_tools"></a>
## Document Source Files and Tools


Project Geode documentation source files include DITA XML topics and maps. Image files include .gif and .png graphics and editable image files in the open source SVG format.

-   [Working with DITA Files](#dita)
-   [Working with Images and Graphics](#images)

<a name="dita"></a>
### Working with DITA Files

We author document content in an XML format called Darwin Information Typing Architecture (DITA). The [DITA standard](http://docs.oasis-open.org/dita/v1.2/os/spec/DITA1.2-spec.html "DITA 1.2 Specification") is maintained by the Organization for the Advancement of Structured Information Standards (OASIS).

You can edit DITA files in any text editor, but XML editors allow you to modify tags easily while conforming to the DITA DTD and schemas. We **strongly** recommend that you use a validating XML editor to ensure that you produce error-free DITA XML. A DITA file must be both well-formed XML and valid, or it can break the build for the entire documentation set.

Some XML editors that support DITA are:

-   [oXygen XML Author](http://www.oxygenxml.com "oXygen XML Author")
-   [XMLMind XML Editor](http://www.xmlmind.com/xmleditor "XMLMind XML Editor")
-   [PTC Arbortext Editor](http://www.ptc.com/product/arbortext/author/editor "Arbortext Editor")
-   [Altova Authentic](http://www.altova.com/authentic.html "Altova Authentic")
-   [Vex (Visual Editor for Eclipse)](http://www.eclipse.org/vex/ "Vex - A Visual Editor for XML")

DITA is a topic-oriented authoring system. A topic is a unit of content that covers a single subject. DITA has four standard topic document types: \<topic\>, \<concept\>, \<reference\>, and \<task\>.

-   The \<topic\> type is a generic document type, and the base from which other document types are derived. It is the least restrictive topic type and has tags similar to (but not identical to) the HTML/XHTML tag set.

-   The \<concept\> document type contains overview or background information that explains what a feature does, when to use it, why it is important, and so on.

-   The \<task\> document type contains step-by-step instructions to accomplish a procedure.

-   The \<reference\> document type provides detailed information for commands, classes, APIs, configuration files, and other objects.

A DITA map is an XML document that orders a collection of topics and creates a topic hierarchy. A map can contain references to topics or to other maps. Maps control the generation of both HTML help and PDFs. Project Geode uses the \<bookmap\> document type for maps, because it includes a \<bookmeta\> section that supplies the metadata needed to produce PDFs, such as the document title, copyright date, and revision numbers.

You can produce output from the DITA sources using the DITA Open Toolkit (DITA-OT) or use the DITA transformations built into your XML editor. We generate end-user documentation using a Ruby gem, Bookbinder, which performs the DITA transformations and also processes sources in HTML and markdown to produce a deployable Web application. See the [README](README.md) file for Bookbinder instructions.

To learn more about DITA markup, how to create and edit DITA files, and find more XML editors that support DITA, visit the [DITA online community](http://dita.xml.org).

<a name="images"></a>
### Working with Images and Graphics

Image files in .gif or .png format are in the `images` directory in the Project Geode docs repo. Images in .svg format are in the `images_svg` directory.

Most of the Project Geode image files have been converted to the open source SVG format. You can insert SVG images directly into an XML topic and modify images using a SVG editor.

The Wikipedia page [Comparison of vector graphics editors](http://en.wikipedia.org/wiki/Comparison_of_vector_graphics_editors) provides a list and comparison of commercial and free vector graphics editors. Note, however, that not all of these programs support the SVG format.

The [Inkscape](https://inkscape.org) vector graphics editor is a popular open source editor that runs on Linux, Windows, and Mac OS X. Following are some tips for working with Inkscape on Mac OS X:

-   If your windows are disappearing (for example you launch Document Properties or try to open a SVG file and the dialog never appears) try this: in Mission Control, turn off "Displays have separate spaces" if you need to use X11 across multiple displays. There are more solutions here: https://bugs.launchpad.net/inkscape/+bug/1244397
-   To resize the canvas to fit the SVG (eliminate all the paper-sized whitespace around the image), choose *Document Properties \> Custom Size \> Resize page to content... Resize page to drawing or selection*.

<a name="guidelines"></a>
## Writing Guidelines

The most important advice we can provide for working with the Project Geode docs is to spend some time becoming familiar with the existing source files and the structure of the project directory. In particular, note the following conventions and tips:

-   Top-level subdirectories organize topics into "books": basic\_config, configuring, developing, etc.

-   Each book has a `book_intro.xml` topic and a `.ditamap`.

-   The second-level subdirectories organize topics into chapters. There is a `chapter_overview.xml` topic for each chapter.

-   Use lowercase characters for all file and directory names. Separate words in filenames with an underscore (`_`) character.

-   Use the `.xml` file extension for topic files and `.ditamap` for map files.

-   Add new topics to the existing directories by subject type. Only create a new directory if you are starting a new subject or a new book.

-   To start a new topic, you can make a copy of an existing file with similar content and edit it.

-   Use the appropriate document type for the content you are writing. Create multiple topics if you are writing overview, procedural, and reference content.

-   Be sure that \<topic\> and \<section\> tags have an id attribute with a value that is unique within the file. Your XML editor may generate id attributes automatically.

-   Instead of typing "Project Geode" or "Geode" into a document, use a \<keyword\> tag. For local builds, you can change the definitions for these and other keywords by editing their values in `Geode.ditamap`.

    -   To produce the phrase "Project Geode", enter: `<keyword keyref="product_name_long"\>`
    -   To produce the phrase "Geode", enter: `<keyword keyref="product_name_short"\>`

-   Each topic should have a \<shortdesc\> tag following the \<title\> tag to describe the purpose of the topic. This content is included with generated topic lists and is an important element of the documentation navigation. Review existing topics for examples.

## Copyright

Copyright © 2015 Pivotal Software, Inc. All rights reserved.

Pivotal Software, Inc. believes the information in this publication is accurate as of its publication date. The information is subject to change without notice. THE INFORMATION IN THIS PUBLICATION IS PROVIDED "AS IS." PIVOTAL SOFTWARE, INC. ("Pivotal") MAKES NO REPRESENTATIONS OR WARRANTIES OF ANY KIND WITH RESPECT TO THE INFORMATION IN THIS PUBLICATION, AND SPECIFICALLY DISCLAIMS IMPLIED WARRANTIES OF MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.

Use, copying, and distribution of any Pivotal software described in this publication requires an applicable software license.

All trademarks used herein are the property of Pivotal or their respective owners.
