Tinkerpop MapDB
---------------

[![Build Status](https://travis-ci.org/jkschneider/tinkerpop-mapdb.svg?branch=master)](https://travis-ci.org/jkschneider/tinkerpop-mapdb)

Introduction
------------

The use case for a Tinkerpop MapDB backend is similar to that of the experimental Titan Infinispan backend, but for graphs that do not require horizontal scaling.  The goal of this implementation is to provide an off-heap in-memory Tinkerpop implementation that is generally faster than Titan Infinispan for suitable graphs.

This is a MapDB backend for Tinkerpop3 tailored toward a memory direct off heap graph that is destroyed on JVM shutdown.  This backend is ideal for Event Sourced applications that are employing graph data to service one or more read models where such read models fit within the host's available memory.  Such graphs can easily grow into the tens of millions of vertices before requiring horizontal scaling.

Getting Started
---------------

Gradle:

```groovy
compile 'io.jons:tinkerpop-mapdb:0.1.+'
```

Maven:

```xml
<dependency>
  <groupId>io.jons</groupId>
  <artifactId>tinkerpop-mapdb</artifactId>
  <version>0.1.0</version>
</dependency>
```
