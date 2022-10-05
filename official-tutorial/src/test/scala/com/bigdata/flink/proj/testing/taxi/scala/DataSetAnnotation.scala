package com.bigdata.flink.proj.testing.taxi.scala

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.functions.FunctionAnnotation.{ForwardedFields, NonForwardedFields, ReadFields}

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/10 5:35 下午 
 * @desc:
 *
 */

/**
 * 语义注释为 flink 函数运行提供了有关提示，其告诉系统函数，哪些子弹每做修改直接转发到下游。使系统能够推断出哪些字段的分区、分组、排序可以被复用。
 * 可以显著提升程序执行效率
 */
// 上游元祖第一个元素 不做修改直接转发 作为下游元祖第 3 个元素
@ForwardedFields(Array("_1->_3"))
class MyMap extends MapFunction[(Int, Int), (String, Int, Int)]{
  def map(value: (Int, Int)): (String, Int, Int) = {
    ("foo", value._2 / 2, value._1)
  }
}

/**
 * 上游第二个元素 被修改了 不能直接转发到下游
 * 一旦使用NonForwardedFields 必须包含所有不能直接转发字段，因为其余字段默认都是可以转发字段
 */
@NonForwardedFields(Array("_2")) // second field is not forwarded
class MyMap2 extends MapFunction[(Int, Int), (Int, Int)]{
  def map(value: (Int, Int)): (Int, Int) = {
     (value._1, value._2 / 2)
  }
}

/**
 * 标记函数中有使用 第1个和第4个字段，需要读取输入元祖的的这两个字段，其余的默认都不会使用。
 * 一旦被读取，表明可能被修改，只有未被读取的才会直接往下游转发
 */
@ReadFields(Array("_1; _4")) // _1 and _4 are read and evaluated by the function.
class MyMap3 extends MapFunction[(Int, Int, Int, Int), (Int, Int)]{
  def map(value: (Int, Int, Int, Int)): (Int, Int) = {
    if (value._1 == 42) {
      return (value._1, value._2)
    } else {
      return (value._4 + 10, value._2)
    }
  }
}