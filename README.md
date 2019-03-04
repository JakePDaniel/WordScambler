# Word Scambler

Simple Apache Spark project that solves the Jumble problem. 

## Explanation of the problem
The jumble puzzle is a common newspaper puzzle, it contains a series of anagrams that must be
solved (see https://en.wikipedia.org/wiki/Jumble). To solve, one must solve each of the
individual jumbles. The circled letters are then used to create an additional anagram to be solved.
In especially difficult versions, some of the anagrams in the first set can possess multiple
solutions. To get the final answer, it is important to know all possible anagrams of a given series
of letters.

## Running the project
The project can be run in Intellj or any other IDE if the correct dependencies are on your machine. 
Within the resources directory add any puzzles to be solved in the puzzles.json file. 
The process will output to two files contained in the anagrams and the finalSentences directories

## Frameworks used
* [Apache Spark](https://spark.apache.org/)

## Requirements
- [JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- [Scala 2.12.8](https://www.scala-lang.org/)
- [SBT 1.2.8](https://www.scala-sbt.org/)
