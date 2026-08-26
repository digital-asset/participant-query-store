// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jdk

import java.lang.invoke.MethodHandles
import java.lang.reflect.Modifier
import java.util
import scala.annotation.nowarn
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** This object serves to overcome https://openjdk.org/jeps/396 */
@nowarn("cat=deprecation")
object ModulesOpener:

  enum Mode(val implName: String):
    case Open               extends Mode("implAddOpens")
    case OpenToAllUnnamed   extends Mode("implAddOpensToAllUnnamed")
    case Export             extends Mode("implAddExports")
    case ExportToAllUnnamed extends Mode("implAddExportsToAllUnnamed")

  /** Opens JDK modules which contain the supplied class names.
    *
    * @param classes
    *   the classes to use as modules selector
    * @param mode
    *   the mode to use for module opening
    * @return
    *   true if any modules matched the selection, false otherwise
    */
  def openForClasses(classes: Set[String], mode: Mode): Boolean =
    openForPackages(packagesForClasses(classes.toSeq).toSet, mode)

  /** Opens JDK modules which contain the supplied package names.
    *
    * @param packages
    *   the packages to use as modules selector
    * @param mode
    *   the mode to use for module opening
    * @return
    *   true if any modules matched the selection, false otherwise
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  def openForPackages(packages: Set[String], mode: Mode): Boolean = Try {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    val unsafe          = unsafeField.get(null).asInstanceOf[sun.misc.Unsafe]
    val implLookupField = classOf[MethodHandles.Lookup].getDeclaredField("IMPL_LOOKUP")
    val lookup = unsafe
      .getObject(unsafe.staticFieldBase(implLookupField), unsafe.staticFieldOffset(implLookupField))
      .asInstanceOf[MethodHandles.Lookup]
    val modifiers    = lookup.findSetter(classOf[java.lang.reflect.Method], "modifiers", Integer.TYPE)
    val exportMethod = Class.forName("java.lang.Module").getDeclaredMethod(mode.implName, classOf[java.lang.String])
    modifiers.invoke(exportMethod, Modifier.PUBLIC)

    for (
      module <- getModules;
      name   <- module.getClass.getMethod("getPackages").invoke(module).asInstanceOf[util.Collection[String]].asScala
      if packages.contains(name)
    ) {
      exportMethod.invoke(module, name)
    }

    packages.isEmpty
  }.fold(_ => false, identity)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def getModules = Try {
    val boot = Class.forName("java.lang.ModuleLayer").getMethod("boot").invoke(null)
    boot.getClass.getMethod("modules").invoke(boot).asInstanceOf[util.Collection[?]].asScala
  }.getOrElse(Seq.empty)

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def packagesForClasses(classes: Seq[String]): Seq[String] = classes.flatMap { className =>
    Try {
      val clazz = Class.forName(className)
      if clazz.isEnum then clazz.getMethod("values").invoke(null) else clazz.getDeclaredConstructor().newInstance()
    }.toEither.swap.toSeq.collect {
      case ex: IllegalAccessException => className.split('.').dropRight(1).mkString(".")
    }
  }

end ModulesOpener
