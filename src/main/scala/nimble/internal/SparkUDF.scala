package nimble.internal

import nimble.api.UDF
import org.apache.spark.api.java.function
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


case class SparkUDF(_fn: UDF, _children: Seq[Expression])
  extends Expression
    with NonSQLExpression
    with CodegenFallback {

  private val _evanFn = evalFn(_children)
  private val _unWrapFn = Utils.unWrapFn(dataType)

  override def foldable: Boolean = _fn.foldable

  override def nullable: Boolean = _fn.nullable

  override def eval(input: InternalRow): Any = {
    val result = _evanFn(input)
    _unWrapFn(result)
  }

  override def dataType: DataType = _fn.dataType(_children.map(_.dataType).asJava)

  override def children: Seq[Expression] = _children

  private def evalFn(children: Seq[Expression]): InternalRow => Any = {
    children.size match {
      case 0 =>
        val func = _fn.asInstanceOf[() => Any]
        (_: InternalRow) => func()

      case 1 =>
        val func = _fn.asInstanceOf[Any => Any]
        val child0 = children(0)
        lazy val wrapFn0 = Utils.wrapFn(child0.dataType)
        (input: InternalRow) => {
          func(
            wrapFn0(child0.eval(input)))
        }

      case 2 =>
        val func = function.asInstanceOf[(Any, Any) => Any]
        val child0 = children(0)
        val child1 = children(1)
        lazy val wrapFn0 = Utils.wrapFn(child0.dataType)
        lazy val wrapFn1 = Utils.wrapFn(child1.dataType)
        (input: InternalRow) => {
          func(
            wrapFn0(child0.eval(input)),
            wrapFn0(child1.eval(input)))
        }

      case 3 =>
        val func = function.asInstanceOf[(Any, Any, Any) => Any]
        val child0 = children(0)
        val child1 = children(1)
        val child2 = children(2)
        lazy val wrapFn0 = Utils.wrapFn(child0.dataType)
        lazy val wrapFn1 = Utils.wrapFn(child1.dataType)
        lazy val wrapFn2 = Utils.wrapFn(child2.dataType)
        (input: InternalRow) => {
          func(
            wrapFn0(child0.eval(input)),
            wrapFn1(child1.eval(input)),
            wrapFn2(child2.eval(input)))
        }

      case 4 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any) => Any]
        val child0 = children(0)
        val child1 = children(1)
        val child2 = children(2)
        val child3 = children(3)
        lazy val wrapFn0 = Utils.wrapFn(child0.dataType)
        lazy val wrapFn1 = Utils.wrapFn(child1.dataType)
        lazy val wrapFn2 = Utils.wrapFn(child2.dataType)
        lazy val wrapFn3 = Utils.wrapFn(child3.dataType)
        (input: InternalRow) => {
          func(
            wrapFn0(child0.eval(input)),
            wrapFn1(child1.eval(input)),
            wrapFn2(child2.eval(input)),
            wrapFn3(child3.eval(input)))
        }

      case 5 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
        val child0 = children(0)
        val child1 = children(1)
        val child2 = children(2)
        val child3 = children(3)
        val child4 = children(4)

        lazy val wrapFn0 = Utils.wrapFn(child0.dataType)
        lazy val wrapFn1 = Utils.wrapFn(child1.dataType)
        lazy val wrapFn2 = Utils.wrapFn(child2.dataType)
        lazy val wrapFn3 = Utils.wrapFn(child3.dataType)
        lazy val wrapFn4 = Utils.wrapFn(child4.dataType)

        (input: InternalRow) => {
          func(
            wrapFn0(child0.eval(input)),
            wrapFn1(child1.eval(input)),
            wrapFn2(child2.eval(input)),
            wrapFn3(child3.eval(input)),
            wrapFn4(child4.eval(input)))
        }
    }
  }
}
