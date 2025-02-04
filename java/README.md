# [WIP] GraphAr Java

This directory contains the code and build system for the GraphAr Java library which powered by [Alibaba-FastFFI](https://github.com/alibaba/fastFFI).

NOTE: This project is still under development, and we will release it soon.

## Dependencies

### Java

- JDK 8 or higher
- Maven

### C++

- CMake 3.5 or higher
- [GraphAr C++ library](../cpp/README.md)
- LLVM 11

Tips: 
- To install GraphAr C++ library, you can refer to our [C++ CI](../.github/workflows/ci.yml) on Ubuntu and CentOS;
- To install LLVM 11, you can refer to our [Java CI](../.github/workflows/java.yml) on Ubuntu or compile from source with [this script](https://github.com/alibaba/fastFFI/blob/main/docker/install-llvm11.sh).

## Build, Test and Install

Only support installing from source currently, but we will support installing from Maven in the future.

```shell
git clone https://github.com/alibaba/GraphAr.git
cd GraphAr
git submodule update --init
# Install GraphAr C++ first ...
cd java
export LLVM11_HOME=<path-to-LLVM11-home>  # In Ubuntu, it is at /usr/lib/llvm-11
mvn clean install
```

## How to use

We will provide JavaDoc after we finished. Basically, Java's API are same as C++ library's.
