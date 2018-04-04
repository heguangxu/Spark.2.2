/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.parser

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Base SQL parsing infrastructure. 基础SQL解析基础设施。
 */
abstract class AbstractSqlParser extends ParserInterface with Logging {

  /** Creates/Resolves DataType for a given SQL string.
    * 为给定的SQL字符串创建/解析数据类型。
    * */
  override def parseDataType(sqlText: String): DataType = parse(sqlText) { parser =>
    astBuilder.visitSingleDataType(parser.singleDataType())
  }

  /** Creates Expression for a given SQL string.
    * 为给定的SQL字符串创建表达式。
    * */
  override def parseExpression(sqlText: String): Expression = parse(sqlText) { parser =>
    astBuilder.visitSingleExpression(parser.singleExpression())
  }

  /** Creates TableIdentifier for a given SQL string.
    * 为给定的SQL字符串创建表标识符。
    * */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = parse(sqlText) { parser =>
    astBuilder.visitSingleTableIdentifier(parser.singleTableIdentifier())
  }

  /** Creates FunctionIdentifier for a given SQL string.
    * 为给定的SQL字符串创建FunctionIdentifier。
    * */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    parse(sqlText) { parser =>
      astBuilder.visitSingleFunctionIdentifier(parser.singleFunctionIdentifier())
    }
  }

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
    *
    * 为给定的SQL字符串创建StructType，这是一个逗号分隔的字段定义列表，它将保留正确的Hive元数据。
   */
  override def parseTableSchema(sqlText: String): StructType = parse(sqlText) { parser =>
    StructType(astBuilder.visitColTypeList(parser.colTypeList()))
  }

  /** Creates LogicalPlan for a given SQL string.
    * 根据给定的SQL字符串，创建一个LogicalPlan
    * 逻辑执行计划的生成
    * */
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    // 首先进入parse(sqlText)方法

    // 此时生成的逻辑执行计划成为unresolved logical plan。只是将sql串解析成类似语法树结构的执行计划，
    // 系统并不知道每个词所表示的意思，离真正能够执行还差很远
    astBuilder.visitSingleStatement(parser.singleStatement()) match {
        // 循环每个parse的语句，如果当前语句是LogicalPlan，就返回
      case plan: LogicalPlan => plan
        // 否则，就是不支持的SQL语句
      case _ =>
        val position = Origin(None, None)
        throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
    }
  }

  /** Get the builder (visitor) which converts a ParseTree into an AST.
    * 得到builder（visitor）将ParseTree转换为AST。
    * */
  protected def astBuilder: AstBuilder

  /**
    * 解析Sql语句，生成逻辑执行计划
    * @param command
    * @param toResult
    * @tparam T
    * @return
    */
  protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    logInfo(s"Parsing command: $command")

    // 这里对于SQL语句的解析采用的是ANTLR 4，从这里看出来的ANTLRNoCaseStringStream

    // 创建SqlBaseLexer词法解析器，这里侍弄了spark catalyst
    val lexer = new SqlBaseLexer(new ANTLRNoCaseStringStream(command))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    // 创建SqlBaseParser语法解析器，这一点会先调用SqlBaseParser的构造函数，然后调用父类的super(input)
    val parser = new SqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode 首先，尝试使用可能更快的SLL模式解析。
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode 如果SLL解析错误，那么就是用LL模式
          tokenStream.reset() // rewind input stream 倒带输入流
          parser.reset()

          // Try Again.   用LL模式重新解析
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position)
    }
  }
}

/**
 * Concrete SQL parser for Catalyst-only SQL statements.
  * 具体的SQL解析器，用于仅用于催化的SQL语句。
 */
class CatalystSqlParser(conf: SQLConf) extends AbstractSqlParser {
  val astBuilder = new AstBuilder(conf)
}

/** For test-only.  */
object CatalystSqlParser extends AbstractSqlParser {
  val astBuilder = new AstBuilder(new SQLConf())
}

/**
 * This string stream provides the lexer with upper case characters only. This greatly simplifies
 * lexing the stream, while we can maintain the original command.
 *
 * This is based on Hive's org.apache.hadoop.hive.ql.parse.ParseDriver.ANTLRNoCaseStringStream
 *
 * The comment below (taken from the original class) describes the rationale for doing this:
 *
 * This class provides and implementation for a case insensitive token checker for the lexical
 * analysis part of antlr. By converting the token stream into upper case at the time when lexical
 * rules are checked, this class ensures that the lexical rules need to just match the token with
 * upper case letters as opposed to combination of upper case and lower case characters. This is
 * purely used for matching lexical rules. The actual token text is stored in the same way as the
 * user input without actually converting it into an upper case. The token values are generated by
 * the consume() function of the super class ANTLRStringStream. The LA() function is the lookahead
 * function and is purely used for matching lexical rules. This also means that the grammar will
 * only accept capitalized tokens in case it is run from other tools like antlrworks which do not
 * have the ANTLRNoCaseStringStream implementation.
  *
  * 此字符串流仅为lexer提供大写字符。这大大简化了流的lexing，而我们可以保持原来的命令。
  *
  * 这是基于Hive的org.apache.hadoop.hive.ql.parse.ParseDriver.ANTLRNoCaseStringStream
  *
  * 下面的注释(取自原始类)描述了这样做的基本原理:
  *
  * 这个类为antlr的词法分析部分提供了一个大小写不敏感的标记检查器。在检查词法规则时，通过将令牌流转换为大写字母，
  * 该类确保了词汇规则只需要与大写字母匹配，而不是大写字母和小写字母组合。这纯粹用于匹配词法规则。实际的令牌文本
  * 以与用户输入相同的方式存储，而不实际将其转换为大写。令牌值由超类ANTLRStringStream的use()函数生成。
  * LA()函数是lookahead函数，它纯粹用于匹配词法规则。这也意味着语法只接受大写的令牌，以防它是从其他工具中运行的，
  * 比如antlrworks，它没有ANTLRNoCaseStringStream实现。
 */

private[parser] class ANTLRNoCaseStringStream(input: String) extends ANTLRInputStream(input) {
  override def LA(i: Int): Int = {
    val la = super.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

/**
 * The ParseErrorListener converts parse errors into AnalysisExceptions.
  *
  * ParseErrorListener将解析错误转换为AnalysisExceptions。
 */
case object ParseErrorListener extends BaseErrorListener {
  override def syntaxError(
      recognizer: Recognizer[_, _],
      offendingSymbol: scala.Any,
      line: Int,
      charPositionInLine: Int,
      msg: String,
      e: RecognitionException): Unit = {
    val position = Origin(Some(line), Some(charPositionInLine))
    throw new ParseException(None, msg, position, position)
  }
}

/**
 * A [[ParseException]] is an [[AnalysisException]] that is thrown during the parse process. It
 * contains fields and an extended error message that make reporting and diagnosing errors easier.
 */
class ParseException(
    val command: Option[String],
    message: String,
    val start: Origin,
    val stop: Origin) extends AnalysisException(message, start.line, start.startPosition) {

  def this(message: String, ctx: ParserRuleContext) = {
    this(Option(ParserUtils.command(ctx)),
      message,
      ParserUtils.position(ctx.getStart),
      ParserUtils.position(ctx.getStop))
  }

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(Some(l), Some(p)) =>
        builder ++= s"(line $l, pos $p)\n"
        command.foreach { cmd =>
          val (above, below) = cmd.split("\n").splitAt(l)
          builder ++= "\n== SQL ==\n"
          above.foreach(builder ++= _ += '\n')
          builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
          below.foreach(builder ++= _ += '\n')
        }
      case _ =>
        command.foreach { cmd =>
          builder ++= "\n== SQL ==\n" ++= cmd
        }
    }
    builder.toString
  }

  def withCommand(cmd: String): ParseException = {
    new ParseException(Option(cmd), message, start, stop)
  }
}

/**
 * The post-processor validates & cleans-up the parse tree during the parse process.
 */
case object PostProcessor extends SqlBaseBaseListener {

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: SqlBaseParser.NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(
      ctx: ParserRuleContext,
      stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    parent.addChild(f(new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      SqlBaseParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)))
  }
}
