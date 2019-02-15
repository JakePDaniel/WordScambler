package com.test.spark

import java.nio.file.{Files, Paths}

import com.test.spark.utils.UserDefinedFunctions
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.appName("WordScrambler").master("local[*]").getOrCreate
    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._
    val sqlContext = sparkSession.sqlContext

    val wordFrequencyLines = new String(Files.readAllBytes(Paths.get("src/main/resources/freq_dict.json")))

    val wordFrequencySeq = wordFrequencyLines.replace("{", "").
      replace("}", "").replaceAll("\\s", "").split(",").toSeq

    val wordFrequencyDF = wordFrequencySeq.toDF().withColumn("temp", split($"value", ":")).select(
      regexp_replace($"temp".getItem(0), "\"", "").as("key"),
      $"temp".getItem(1).as("value")
    ).drop("temp")


    //Reading puzzle JSON and making a new row for each puzzle contained in the puzzles file
    val puzzles = sqlContext.read.option("MULTILINE", true).json("src/main/resources/puzzles.json").
      withColumn("puzzle", explode($"puzzles")).drop($"puzzles")


      //Extracting nested data about each puzzle into columns
      .withColumn("finalWordSizes", $"puzzle.finalWordSizes").
      withColumn("puzzleID", $"puzzle.id").
      withColumn("tempScrambledWords", explode($"puzzle.words")).
      withColumn("scrambledWord", $"tempScrambledWords.letters").
      withColumn("circledLettersNumbers", $"tempScrambledWords.circledLetters").
      withColumn("wordID", $"tempScrambledWords.id").
      drop("puzzle", "tempScrambledWords")

      //Taking the scrambled word and making a new row for each permutation
      .withColumn("possibleWord", explode(UserDefinedFunctions.getPermutationsFunction($"scrambledWord")))

      //Taking the possible words and joining them against the frequency dictionary to see if they are real words and
      // what the frequency of the word is
      .join(wordFrequencyDF, upper($"possibleWord") === upper(wordFrequencyDF.col("key")))



    val window = Window.partitionBy($"puzzleID", $"wordID").orderBy($"value".desc)

    //Grabbing the max frequency for all the puzzle's anagrams. For a given puzzle if two solutions have the same
    //frequency only one will be taken. A future addition would be to handle multiple
    var resultsToFirstAnagrams = puzzles.withColumn("frequency", max("value").over(window))
      .filter($"frequency" === $"value").dropDuplicates("puzzleID", "wordId")
      .drop("value")

    resultsToFirstAnagrams.drop("finalWordSizes", "circledLettersNumbers", "key")
      .coalesce(1).write.mode("overwrite").json("anagrams")

      //Goes through each solution and grabs the letters that were circled in the puzzle to be used to create the
      //final anagram
      val finalAnagram = resultsToFirstAnagrams.withColumn("circledLetters", UserDefinedFunctions.getCircledLettersFunction($"possibleWord", $"circledLettersNumbers"))
      .drop($"circledLettersNumbers")


    //Collects all the circled letters from the first anagrams in a puzzle
    .groupBy("puzzleID").
      agg(concat_ws("", collect_list($"circledLetters")).as("finalLetters"), first($"finalWordSizes") as "finalWordSizes")


    //Takes the final letters and gets all permutations of them and then splits the permutation on the sentence sizes
    //of the final anagram.
    .withColumn("possibleWords", explode(UserDefinedFunctions.getPermutationsFunction($"finalLetters")))
      .withColumn("splitWords", explode(UserDefinedFunctions.splitFinalWordsFunction($"possibleWords", $"finalWordSizes")))


    //Takes all the words generated and gets the frequency of them. Filters out any grouping of words
     val finalSentences = finalAnagram.join(wordFrequencyDF,upper(finalAnagram.col("splitWords")) === upper(wordFrequencyDF.col("key")))
      .groupBy($"puzzleId", $"possibleWords")
      .agg(sum($"value") as "totalFrequency",  collect_list($"splitWords") as "finalSentence", first($"finalWordSizes") as "finalWordSizes")
       .withColumn("size", size($"finalSentence")).where($"size" === size($"finalWordSizes"))


    
    val finalSentenceWindow = Window.partitionBy($"puzzleID").orderBy($"totalFrequency".desc)

    //Gets the highest frequency group of words for a given puzzle
    finalSentences.withColumn("maxTotalFrequency", max("totalFrequency").over(finalSentenceWindow))
      .filter($"maxTotalFrequency" === $"totalFrequency").dropDuplicates("puzzleID")
      .drop("totalFrequency").drop("finalWordSizes", "size", "possibleWords").
      coalesce(1).write.mode("overwrite").json("finalSentences")
  }
}