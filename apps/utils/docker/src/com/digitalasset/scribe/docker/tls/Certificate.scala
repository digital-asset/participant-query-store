// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.docker.tls

import zio.ZIO

case class Certificate(certificate: GeneratedCertificate)

object Certificate:
  def generate(
      name: String,
      alternativeSubjectNames: Seq[String] = Seq.empty
  ): ZIO[CertificateAuthority, Throwable, Certificate] = for
    ca   <- CertificateAuthority.get
    cert <- GeneratedCertificate.createCertificate(name, alternativeSubjectNames, Some(ca.certificate), false)
  yield Certificate(cert)
end Certificate
