package nimble.internal

import java.util

import nimble.api.Fn
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


case class SparkUDF(_fn: Fn, _children: Seq[Expression])
  extends Expression
    with NonSQLExpression
    with CodegenFallback {

  private lazy val _eval = evalFn(_fn, _children)
  private lazy val _unwrap = DataTypeWrappers.unwrapFn(dataType)

  override lazy val deterministic: Boolean = _fn.deterministic() && children.forall(_.deterministic)

  override def nullable: Boolean = _fn.nullable

  override def toString: String =
    s"UDF:${_fn.name}(${children.mkString(", ")})"

  override def eval(input: InternalRow): Any = {
    val result = _eval(input)
    _unwrap(result)

  }

  override def dataType: DataType = _fn.returnType(_children.map(_.dataType).asJava)

  override def children: Seq[Expression] = _children

  private def evalFn(fn: Fn, children: Seq[Expression]): InternalRow => Any = {
    children.size match {
      case 0 =>
        val func = fn.asInstanceOf[UDF0[Any]]
        (_: InternalRow) => func.call()

      case 1 =>
        val func = fn.asInstanceOf[UDF1[Any, Any]]
        val child0 = children(0)
        lazy val wrapFn0 = DataTypeWrappers.wrapFn(child0.dataType)
        (input: InternalRow) => {
          func.call(wrapFn0(child0.eval(input)))
        }

      case 2 =>
        val func = fn.asInstanceOf[UDF2[Any, Any, Any]]
        val child0 = children(0)
        val child1 = children(1)
        lazy val wrapFn0 = DataTypeWrappers.wrapFn(child0.dataType)
        lazy val wrapFn1 = DataTypeWrappers.wrapFn(child1.dataType)
        (input: InternalRow) => {
          func.call(
            wrapFn0(child0.eval(input)),
            wrapFn1(child1.eval(input)))
        }

      case 3 =>
        val func = fn.asInstanceOf[UDF3[Any, Any, Any, Any]]
        val child0 = children(0)
        val child1 = children(1)
        val child2 = children(2)
        lazy val wrapFn0 = DataTypeWrappers.wrapFn(child0.dataType)
        lazy val wrapFn1 = DataTypeWrappers.wrapFn(child1.dataType)
        lazy val wrapFn2 = DataTypeWrappers.wrapFn(child2.dataType)
        (input: InternalRow) => {
          func.call(
            wrapFn0(child0.eval(input)),
            wrapFn1(child1.eval(input)),
            wrapFn2(child2.eval(input)))
        }

      case 4 =>
        val func = fn.asInstanceOf[UDF4[Any, Any, Any, Any, Any]]
        val child0 = children(0)
        val child1 = children(1)
        val child2 = children(2)
        val child3 = children(3)
        lazy val wrapFn0 = DataTypeWrappers.wrapFn(child0.dataType)
        lazy val wrapFn1 = DataTypeWrappers.wrapFn(child1.dataType)
        lazy val wrapFn2 = DataTypeWrappers.wrapFn(child2.dataType)
        lazy val wrapFn3 = DataTypeWrappers.wrapFn(child3.dataType)
        (input: InternalRow) => {
          func.call(
            wrapFn0(child0.eval(input)),
            wrapFn1(child1.eval(input)),
            wrapFn2(child2.eval(input)),
            wrapFn3(child3.eval(input)))
        }

      case 5 =>
        val func = fn.asInstanceOf[UDF5[Any, Any, Any, Any, Any, Any]]
        val child0 = children(0)
        val child1 = children(1)
        val child2 = children(2)
        val child3 = children(3)
        val child4 = children(4)

        lazy val wrapFn0 = DataTypeWrappers.wrapFn(child0.dataType)
        lazy val wrapFn1 = DataTypeWrappers.wrapFn(child1.dataType)
        lazy val wrapFn2 = DataTypeWrappers.wrapFn(child2.dataType)
        lazy val wrapFn3 = DataTypeWrappers.wrapFn(child3.dataType)
        lazy val wrapFn4 = DataTypeWrappers.wrapFn(child4.dataType)

        (input: InternalRow) => {
          func.call(
            wrapFn0(child0.eval(input)),
            wrapFn1(child1.eval(input)),
            wrapFn2(child2.eval(input)),
            wrapFn3(child3.eval(input)),
            wrapFn4(child4.eval(input)))
        }
    }
  }
}
