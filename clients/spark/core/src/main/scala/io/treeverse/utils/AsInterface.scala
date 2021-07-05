package io.treeverse.utils

import org.objectweb.asm.ClassVisitor
import org.objectweb.asm.Opcodes.ASM4
import org.objectweb.asm.Type

private class AsInterface(cv: ClassVisitor, iface: Class[_]) extends ClassVisitor(ASM4, cv) {
  override def visit(
    version: Int,
    access: Int,
    name: String,
    signature: String,
    superName: String,
    interfaces: Array[String]
  ) = {
    val newInterfaces = interfaces :+ Type.getType(iface).getInternalName
    cv.visit(version, access, name, signature, superName, newInterfaces)
  }
}
