// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.mill

package object junitxml {
  case class Test(
      fullyQualifiedName: String,
      selector: String,
      duration: Double,
      failure: Option[Failure],
      ignored: Boolean
  )
  case class Failure(name: String, message: String, trace: Seq[Trace])
  case class Trace(declaringClass: String, methodName: String, fileName: String, lineNumber: Int) {
    override def toString: String = s"$declaringClass.$methodName($fileName:$lineNumber)"
  }

  def saveJunitReport(
      suiteId: String,
      suiteName: String,
      reports: Seq[mill.testrunner.TestResult],
      into: os.Path
  ): Unit = {
    val tests = reports.map(report =>
      Test(
        report.fullyQualifiedName,
        selector = report.selector,
        duration = report.duration / 1000.0,
        failure =
          if (Seq("Success", "Ignored").contains(report.status))
            None
          else
            Some(
              Failure(
                name = report.exceptionName.getOrElse("UNKNOWN"),
                message = fansi.Str.Strip(report.exceptionMsg.getOrElse("UNKNOWN")).plainText,
                trace = report.exceptionTrace.toList.flatten.map(trace =>
                  Trace(
                    declaringClass = trace.getClassName,
                    methodName = trace.getMethodName,
                    fileName = trace.getFileName,
                    lineNumber = trace.getLineNumber
                  )
                )
              )
            ),
        ignored = report.status == "Ignored"
      )
    )

    val xml =
      <testsuites
      id={suiteId}
      name={suiteName}
      tests={tests.length.toString}
      skipped={tests.count(_.ignored).toString}
      failures={tests.count(_.failure.isDefined).toString}
      time={tests.map(_.duration).sum.toString}
      >{
        tests.groupBy(_.fullyQualifiedName).map {
          case (suit, tests) =>
            <testsuite
            id={suit}
            name={suit}
            tests={tests.length.toString}
            skipped={tests.count(_.ignored).toString}
            failures={tests.count(_.failure.isDefined).toString}
            time={tests.map(_.duration).sum.toString}>{
              tests.map { test =>
                <testcase
                id={s"${test.fullyQualifiedName}#${test.selector}"}
                classname={test.fullyQualifiedName}
                name={test.selector}
                time={test.duration.toString}>{
                  test.failure.map { failure =>
                    <failure message={failure.name} type="ERROR">{failure.message}

                      Stacktrace:
                      {failure.trace.mkString("\n")}</failure>
                  }.orNull
                  if (test.ignored) <skipped message="Test was skipped." />
                }</testcase>
              }
            }</testsuite>
        }
      }</testsuites>

    scala.xml.XML.save(filename = into.toIO.getCanonicalPath, node = xml, xmlDecl = true)
  }

}
