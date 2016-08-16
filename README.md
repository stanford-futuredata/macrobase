# MacroBase

[![Build Status](https://travis-ci.org/stanford-futuredata/macrobase.svg)](https://travis-ci.org/stanford-futuredata/macrobase)
[![Coverage Status](https://coveralls.io/repos/github/stanford-futuredata/macrobase/badge.svg?branch=master)](https://coveralls.io/github/stanford-futuredata/macrobase?branch=master)

## What is MacroBase?

MacroBase is a new *analytic monitoring* engine designed to prioritize human attention in large-scale datasets and data streams. Unlike a traditional analytics engine, MacroBase is specialized for one task: finding and explaining unusual or interesting trends in data.

Under the hood, MacroBase is architected for rapid adaptation to new application domains. MacroBase has been successfully used for analyses in domains including datacenter and mobile application monitoring, industrial manufacturing, and video and satellite image analysis.

MacroBase supports streaming operation, meaning that once users identify an important behavior, they can track it continuously over time.

## How can I run MacroBase queries?

The simplest way to use MacroBase is through its UI. Users highlight key performance metrics (e.g., power drain, latency) and metadata attributes (e.g., hostname, device ID), and MacroBase reports explanations of abnormal behavior. For example, MacroBase may report that queries running on host 5 are 10 times more likely to experience high latency than the rest of the cluster or that devices of type X103 are 30 times more likely to experience high power drain. Check out the [Tutorial](https://github.com/stanford-futuredata/macrobase/wiki/Tutorial).

MacroBase is designed for extensibility, and there are a number of more advanced interfaces you can use to program the system and execute more complex query functionality. We have some [basic notes](https://github.com/stanford-futuredata/macrobase/wiki/Running-MacroBase-Queries) and will be adding more shortly.

## Who can I contact? How do I find out more?

Feel free to open GitHub issues or email us at macrobase@cs.stanford.edu.
The development branches contain most of the action.
A detailed technical overview of the project is available [on the arXiv](http://arxiv.org/pdf/1603.00567.pdf).
