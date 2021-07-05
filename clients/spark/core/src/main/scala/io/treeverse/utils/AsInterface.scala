package io.treeverse.utils

import org.objectweb.asm.{ClassVisitor, MethodVisitor}
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

  val descReturnType = """\)L([a-zA-Z_0-9/]+);""".r
  def translateDesc(desc: String) =
    descReturnType.replaceAllIn(desc, _ match { case (name) => /* BUG */ ")L" + iface.getName.replaceAll("""\.""", "/") + ";" })

  override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]): MethodVisitor = {
    Console.out.println(s"[DEBUG] name ${name} desc ${desc} signature ${signature}")
    val mv = cv.visitMethod(access, name, translateDesc(desc), signature, exceptions)
    return mv
  }
}
