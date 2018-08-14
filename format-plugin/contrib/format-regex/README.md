# Example Drill Regex Format Plugin

The files in this directory show how to build a simple format plugin for
Drill based on the "easy" format plugin structure.

## Building the Plugin

For various reasons, "easy" plugins cannot yet be built as separate projects.
To build this, copy it into an existing Drill source structure, rooted at
`$DRILL_ROOT`:

`$DRILL_ROOT/contrib`

Then, add the following to the `$DRILL_ROOT/contrib/pom.xml` file:

```
  <modules>
    ...
    <module>format-regex</module>
  </modules>
```

Add just the `<module>...</module>` line.

The above change tells Maven to build `format-regex` as part of the normal Drill build.

Then, build Drill:

```
$ cd $DRILL_ROOT
$ mvn clean install -DskipTests
```
## Usage

See Chapter 12, "Format Plugins" of the O'Reilly book, "Learning Apache Drill" for details
on how to this example is created as a guide to building our own. The book also explains
how to run tests, and how to configure the plugin.
