// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.docker.tls
import org.bouncycastle.asn1.x500.*
import org.bouncycastle.asn1.x509.*
import org.bouncycastle.cert.jcajce.*
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.bouncycastle.util.IPAddress
import org.bouncycastle.util.encoders.Base64
import com.nimbusds.jose.jwk.*
import java.security.cert.X509Certificate
import java.security.{KeyPairGenerator, KeyStore, PrivateKey}
import java.security.interfaces.RSAPublicKey
import zio.{Task, ZIO}
import java.io.ByteArrayOutputStream
import java.math.BigInteger
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date
import scala.util.Using

final case class GeneratedCertificate(privateKey: PrivateKey, certificate: X509Certificate) {
  def der: Array[Byte] = privateKey.getEncoded
  def pem: String      = s"-----BEGIN PRIVATE KEY-----\n${encode(privateKey.getEncoded)}\n-----END PRIVATE KEY-----"
  def crt: String      = s"-----BEGIN CERTIFICATE-----\n${encode(certificate.getEncoded)}\n-----END CERTIFICATE-----"

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.TryPartial"))
  def pkcs12: Array[Byte] = {
    val ks = KeyStore.getInstance("PKCS12")
    ks.load(null, null)
    ks.setKeyEntry("key", privateKey, null, Array(certificate))
    Using(new ByteArrayOutputStream()) { baos =>
      ks.store(baos, Array.empty)
      baos.toByteArray
    }.get
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def toJWK: String =
    RSAKey
      .Builder(certificate.getPublicKey.asInstanceOf[RSAPublicKey])
      .privateKey(privateKey)
      .keyUse(KeyUse.SIGNATURE)
      .issueTime(new Date())
      .build()
      .toJSONString

  private def encode(bytes: Array[Byte]): String = new String(Base64.encode(bytes))
}

object GeneratedCertificate:
  def createCertificate(
      cnName: String,
      alternativeSubjectNames: Seq[String],
      issuer: Option[GeneratedCertificate],
      isCA: Boolean
  ): Task[GeneratedCertificate] = ZIO.attempt {
    // Generate the key-pair with the official Java API's
    val keyGen       = KeyPairGenerator.getInstance("RSA")
    val certKeyPair  = keyGen.generateKeyPair()
    val name         = new X500Name("CN=" + cnName)
    val serialNumber = BigInteger.valueOf(System.currentTimeMillis())
    val validFrom    = Instant.now()
    val validUntil   = validFrom.plus(10 * 360, ChronoUnit.DAYS)

    val (issuerName, issuerKey) = issuer.fold( // self-sign
      (name, certKeyPair.getPrivate)
    )(issuer => // chain-sign
      (new X500Name(issuer.certificate.getSubjectX500Principal.getName), issuer.privateKey)
    )

    // The cert builder to build up our certificate information
    val builder = new JcaX509v3CertificateBuilder(
      issuerName,
      serialNumber,
      Date.from(validFrom),
      Date.from(validUntil),
      name,
      certKeyPair.getPublic
    )

    // Make the cert to a Cert Authority to sign more certs when needed
    if isCA then builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCA))

    // Add domain information
    if alternativeSubjectNames.nonEmpty then
      builder.addExtension(
        Extension.subjectAlternativeName,
        false,
        new GeneralNames(
          alternativeSubjectNames.map {
            case ip if IPAddress.isValidIPv4(ip) => new GeneralName(GeneralName.iPAddress, ip)
            case dns                             => new GeneralName(GeneralName.dNSName, dns)
          }.toArray
        )
      )

    // Finally, sign the certificate:
    val signer     = new JcaContentSignerBuilder("SHA256WithRSA").build(issuerKey)
    val certHolder = builder.build(signer)
    val cert       = new JcaX509CertificateConverter().getCertificate(certHolder)

    new GeneratedCertificate(certKeyPair.getPrivate, cert)
  }
end GeneratedCertificate
