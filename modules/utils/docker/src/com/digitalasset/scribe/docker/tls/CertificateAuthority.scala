// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.docker.tls
import zio.{Task, URIO, ZEnvironment, ZIO, ZLayer}

final case class CertificateAuthority(certificate: GeneratedCertificate, additionalSubjects: Seq[String]) {
  def generate(name: String, alternativeSubjectNames: Seq[String] = Seq.empty): Task[Certificate] =
    Certificate
      .generate(
        name,
        if alternativeSubjectNames.isEmpty then alternativeSubjectNames
        else alternativeSubjectNames ++ additionalSubjects
      )
      .provideEnvironment(ZEnvironment(this))
}

object CertificateAuthority:
  def generate(additionalSubjects: Seq[String]): ZLayer[Any, Throwable, CertificateAuthority] = ZLayer.fromZIO(
    GeneratedCertificate
      .createCertificate("authority", Seq.empty, None, true)
      .map(CertificateAuthority(_, additionalSubjects))
  )

  def get: URIO[CertificateAuthority, CertificateAuthority] = ZIO.service[CertificateAuthority]
end CertificateAuthority
