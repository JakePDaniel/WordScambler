package com.test.spark.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

/**
  * Class containing all the user defined functions to be used in spark
  */
object UserDefinedFunctions {


  /**
    * Wrapper function to get all the permutations of a given string.
    *
    * @return Array containing all the permutations of the string
    */
  private def getAllPermutations = (letters: String) => {
    letters.toList.permutations.map(_.mkString).toArray
  }

  /**
    * Returns the letters that were circled in the puzzle
    *
    * @return string containing all the letters that were circled
    */
  private def getCircledLetters = (word: String, indexes : mutable.WrappedArray[String]) =>{
    val letters = new StringBuilder
    indexes.foreach(index =>{
      letters.append(word.charAt(index.toInt).toString)
    })
    letters.toString()
  }

  /**
    * Splits the final words based on the word sizes in the final sentence
    *
    * @return List of the spilt words
    */
  private def splitFinalWords = (combinedWords: String, indexes : mutable.WrappedArray[String]) =>{
    val splitWords = new mutable.MutableList[String]
    var startOfWord = 0
    var endOfWord = 0
    indexes.foreach(index =>{
      endOfWord = index.toInt + endOfWord
      splitWords += combinedWords.substring(startOfWord, endOfWord)
      startOfWord = endOfWord
    })
    splitWords
  }


  val getPermutationsFunction: UserDefinedFunction = udf(getAllPermutations)
  val getCircledLettersFunction : UserDefinedFunction = udf(getCircledLetters)
  val splitFinalWordsFunction : UserDefinedFunction = udf(splitFinalWords)

}
