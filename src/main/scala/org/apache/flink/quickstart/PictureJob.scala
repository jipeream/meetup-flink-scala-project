package org.apache.flink.quickstart

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import org.apache.flink.api.scala._
import scala.util.control.Exception.allCatch

/**
  * Skeleton for a Flink Job.
  *
  * For a full example of a Flink Job, see the WordCountJob.scala file in the
  * same package/directory or have a look at the website.
  *
  * You can also generate a .jar file that you can submit on your Flink
  * cluster. Just type
  * {{{
  *   mvn clean package
  * }}}
  * in the projects root directory. You will find the jar in
  * target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
  *
  */
object PictureJob {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val pictures = env.readCsvFile[Picture](
      filePath = "src/main/resources/pictures.csv",
      ignoreFirstLine = true,
      pojoFields = Array("name", "year", "nominationsStr", "rating", "duration", "genre1", "genre2", "release", "metacriticStr", "synopsis")
    )

    //    pictures.print()
    //    pictures.count()

    val validNominations = pictures.filter(p => Utils.isInteger(p.nominationsStr)).map(p => p.nominationsStr.toDouble)
    println(validNominations.reduce(_ + _).collect().head / validNominations.count)

    //val validNominations = pictures.filter(Utils.isInteger(_.nominationsStr)).map(_.nominationsStr.toDouble)
    //println(validNominations.reduce(_ + _).collect().head / validNominations.count)

    //val validNominations = pictures.filter(p => Utils.isInteger(p.nominationsStr)).map(p => Tuple1(p.nominationsStr.toDouble))
    //println(validNominations.sum(0).collect().head._1 / validNominations.count())

    //val validNominations = pictures.filter(Utils.isInteger(_.nominationsStr)).map(Tuple1(_.nominationsStr.toDouble))
    //println(validNominations.sum(0).collect().head._1 / validNominations.count())

    //val validNominations = pictures.filter(!_.nominations.isEmpty).map(p => Tuple1[Double](p.nominations.get))
    //println(validNominations.sum(0).collect().head._1 / validNominations.count())

    //val validNominations = pictures.filter(_.nominations > 0)
    //println(validNominations.sum("nominations").collect().head.nominations / validNominations.count)

    //// execute program
    //env.execute("Flink Scala API Skeleton")
  }
}

object Utils {
  def isInteger(s: String): Boolean = (allCatch opt s.toInt).isDefined
  def isDouble(s: String): Boolean = (allCatch opt s.toDouble).isDefined
}

case class Picture(
  val name: String, // Slumdog Millionaire
  val year: Integer, // 2008
  // val nominations:Integer,
  // val nominations:Option[Integer],
  val nominationsStr: String, // 10
  val rating: Double, // 8
  val duration: Integer, // 120
  val genre1: String, // Drama
  val genre2: String, // Romance
  val release: String, // January
  // val metacritic:Integer,
  // val metacritic:Option[Integer],
  val metacriticStr: String, // 86
  val synopsis: String // "A Mumbai teen reflects on his upbringing in the slums when he is accused of cheating on the Indian Version of ""Who Wants to be a Millionaire?"""
) {
  def nominations: Option[Double] = if ((allCatch opt nominationsStr.toDouble).isDefined) None else Some(nominationsStr.toDouble)
}