package org.uam.masterbigdata.api.documentation

trait ApiMapper {
  private[api] lazy val mapToId: Long => Long = id => id

  private[api] lazy val mapToLabel: String => String = label => label

  private[api] lazy val mapToNothing: Unit => Unit = nothing => nothing
}
