# Borrowed from https://nathanleclaire.com/blog/2014/07/12/10-docker-tips-and-tricks-that-will-make-you-sing-a-whale-song-of-joy/
FROM ubuntu:16.04

# Install dependencies, including Java 8 and Maven 3
RUN apt-get update && apt-get install -y \
 build-essential \
 cmake \
 curl \
 git \
 less \
 man-db \
 maven \
 openjdk-8-jdk \
 pkg-config \
 screen \
 sudo \
 tmux \
 vim \
 wget

# Setup home environment and password-less sudo access for user "ubuntu"
RUN adduser --home /home/ubuntu --disabled-password --gecos '' ubuntu
RUN adduser ubuntu sudo
RUN echo "%sudo ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
WORKDIR /home/ubuntu
ENV HOME /home/ubuntu
USER ubuntu

# Setup MacroBase
RUN cd /home/ubuntu && wget https://github.com/stanford-futuredata/macrobase/archive/v1.0.tar.gz
RUN cd /home/ubuntu && tar xvf v1.0.tar.gz
RUN cd /home/ubuntu/macrobase-1.0 && ./build.sh core sql

