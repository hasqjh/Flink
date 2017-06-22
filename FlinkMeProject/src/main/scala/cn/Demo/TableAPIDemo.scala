package cn.Demo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Table, TableEnvironment}

/**
  * Created by Administrator on 2017/1/23.
  */
case class WC(word: String, count: Int)

object TableAPIDemo {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))

    //---------------------------------------------------------------
    //DataSet转换成Table，而非注册
    val expr = input.toTable(tEnv)
    val result = expr
      .groupBy('word)
      .select('word, 'count.sum as 'count)
      .toDataSet[WC]

    result.print()

    //-----------------------------------------------------------------
    //DataSet注册成表进行Sql批量执行查询
    tEnv.registerDataSet("bat", input, 'name, 'count)
    val sql: Table = tEnv.sql("select * from bat")
    val set: DataSet[WC] = sql.toDataSet[WC]
    set.print()

  }

}
