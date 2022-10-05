package com.bigdata.flink.proj.testing.taxi.scala

// 一次性导入Table Api 全部类型
import org.apache.flink.table.annotation.{DataTypeHint, HintFlag}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.DataTypes._
import org.apache.flink.table.types.DataType

import java.sql.Timestamp


class DataTypeTest {

  /**
   * 早期 flink table api 和 sql 的类型与 TypeInformartion 耦合，在flink 1.12.x 才分拆
   */
  def dataType(): Unit ={
    // interval 的上线边界
    val interval = INTERVAL(DAY(),SECOND(3))
  }

  /**
   * 在 Table 编程环境中，基于 SQL 的类型系统与程序指定的数据类型之间需要物理提示。该提示指出了实现预期的数据格式。
   * 类型提示：告诉程序转换成预期类型。只在api 扩展时使用，在自定义source、sink、function时可以不使用
   */
  def bridgeTo(): Unit ={
    val type1:DataType = DataTypes.TIMESTAMP().bridgedTo(classOf[Timestamp]) // 告诉程序在生产和消费时，关于时间戳，当初java.sql.Timestamp处理，不要默认LocalDateTime 处理
    val type2:DataType = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(classOf[Array[Int]]) // 不要按java.lang.Interger处理，按scala.Int处理
  }

  // 对于某些类，需要有注解才能将类映射到数据类型（例如， @DataTypeHint("DECIMAL(10, 2)") 为 java.math.BigDecimal 分配固定的精度和小数位）。
  // 任意序列化类型的数据类型。此类型对于 Flink Table 来讲是一个黑盒子，仅在跟外部交互时被反序列化。
  def dataTypeHint(): Unit ={
    DataTypes.of(classOf[UserX])
  }

}


case class UserX(
   age:Int,
   name:String,
   @DataTypeHint("DECIMAL(10,2)") totalBalance:BigDecimal,
   @DataTypeHint("RAW") modelClass:Class[_]
 )

/**
 * Flink API 经常尝试使用反射自动从类信息中提取数据类型，以避免重复的手动定义模式工作。然而以反射方式提取数据类型并不总是成功的，
 * 因为可能会丢失逻辑信息。因此，可能有必要在类或字段声明附近添加额外信息以支持提取逻辑。
 */
case class UserY(

  // defines an INT data type with a default conversion class `java.lang.Integer`
  @DataTypeHint("INT")
  var o1: AnyRef,

  // defines a TIMESTAMP data type of millisecond precision with an explicit conversion class
  @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = classOf[Timestamp])
  var o2: AnyRef,

  // enrich the extraction with forcing using a RAW type
  @DataTypeHint("RAW")
  var modelClass: Class[_],

  // defines that all occurrences of java.math.BigDecimal (also in nested fields) will be
  // extracted as DECIMAL(12, 2)
  @DataTypeHint(defaultDecimalPrecision = 12, defaultDecimalScale = 2)
  var stmt: AccountStatement,

  // defines that whenever a type cannot be mapped to a data type, instead of throwing
  // an exception, always treat it as a RAW type
  @DataTypeHint(allowRawGlobally = HintFlag.TRUE)
  var model: ComplexModel,
)

class ComplexModel()
class AccountStatement()
