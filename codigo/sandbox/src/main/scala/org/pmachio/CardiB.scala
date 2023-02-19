package org.pmachio

object CardiB {
  def realName(): String = {
    "Belcalis Almanzar"
  }
  def iLike(args: String*): String = {
    if (args.isEmpty) {
      throw new java.lang.IllegalArgumentException
    }
    "I like " + args.mkString(", ")
  }
}
