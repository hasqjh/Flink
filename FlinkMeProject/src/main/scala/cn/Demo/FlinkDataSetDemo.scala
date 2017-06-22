package cn.Demo

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

/**
  * Implements the "WordCount" program that computes a simple word occurrence histogram
  * over some sample data
  *
  * This example shows how to:
  *
  * - write a simple Flink program.
  * - use Tuple data types.
  * - write and use user-defined functions.
  */
object FlinkDataSetDemo {
  def main(args: Array[String]) {
    //val params: ParameterTool = ParameterTool.fromArgs(args)
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    // get input data
    val text = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")
    val path = "I:\\wordcount.txt"
    val text2 = env.readTextFile(path)

    /* val counts = text.flatMap {
      _.toLowerCase.split(" ")
    }.map {
        (_, 1)
      }.groupBy(0).sum(1)
    counts.print()*/
    /*val distinct: DataSet[String] = text.flatMap { _.toLowerCase.split(" ") }.distinct()
    distinct.print()*/
    //Aggregates
    /*  val max: AggregateDataSet[(String, Int)] = counts.max(1)
      max.print()*/


    //join
    /*val join1: DataSet[(String, Int, Int)] = text.flatMap {
      _.toLowerCase.split(" ")
    }.map((_,1,1))
    val join2: DataSet[(String, Int, Int)] = text2.flatMap {
      _.toLowerCase.split(" ")
    }.map((_,2,2))
    val joinRes: JoinDataSet[(String, Int, Int), (String, Int, Int)] = join1.join(join2).where(0).equalTo(0) //key of the first input (tuple field 0)  // key of the second input (tuple field 1)
    joinRes.print()
    */
    // This executes a join by broadcasting the first data set
    // using a hash table for the broadcasted data
    /*   joinRes = join1.join(join2, JoinHint.BROADCAST_HASH_FIRST).where(0).equalTo(1);
   */



    // emit result
    /* counts.writeAsCsv("I:\\wordcountress", "\n", " ")
     env.execute("Scala WordCount Example")*/
    // execute program
  }
}
