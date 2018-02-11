# Borrowed from https://nathanleclaire.com/blog/2014/07/12/10-docker-tips-and-tricks-that-will-make-you-sing-a-whale-song-of-joy/
FROM ubuntu:16.04

# Install dependencies, including Java 8 and Maven 3
RUN apt-get update -y
RUN apt-get install -y openjdk-8-jdk
RUN apt-get install -y maven
RUN apt-get install -y wget
RUN apt-get install -y curl
RUN apt-get install -y vim
RUN apt-get install -y pkg-config
RUN apt-get install -y cmake
RUN apt-get install -y build-essential
RUN apt-get install -y tmux
RUN apt-get install -y screen

# Setup home environment
RUN useradd ubuntu
RUN mkdir /home/ubuntu && chown -R ubuntu: /home/ubuntu
WORKDIR /home/ubuntu
ENV HOME /home/ubuntu
ADD .bashrc /home/ubuntu/.bashrc
USER ubuntu

# Setup MacroBase
RUN cd /home/ubuntu && wget https://github.com/stanford-futuredata/macrobase/archive/v0.5.tar.gz
RUN cd /home/ubuntu && tar xvf v0.5.tar.gz
RUN cd /home/ubuntu/macrobase-0.5 && ./build.sh core sql

# Create a shared data volume. We need to create an empty file, otherwise the
# volume will belong to root. (This is probably a Docker bug.)
RUN mkdir /var/shared/
RUN touch /var/shared/placeholder
RUN chown -R ubuntu:ubuntu /var/shared
VOLUME /var/shared

