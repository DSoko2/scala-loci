package retier
package ide.intellij

import typeconstraints._
import scala.language.implicitConversions

protected[retier] trait ImplicitConversions {
  @annotation.compileTimeOnly("Used to guide IntelliJ IDEA Scala Plugin type inference. Do not use directly.")
  final implicit def $$retier$valuePlacedNonIssued[P <: Peer, T]
    (v: T localOn P)
    (implicit
        dummy: IntelliJDummy,
        ev0: LocalPeer[P],
        ev1: T <:!< (_ <=> _),
        ev2: T <:!< (_ <-> _)): T = ???

  @annotation.compileTimeOnly("Used to guide IntelliJ IDEA Scala Plugin type inference. Do not use directly.")
  final implicit def $$retier$valuePlacedControlledIssued[P <: Peer, R <: Remote[Peer], T, U]
    (v: T localOn P)
    (implicit
        dummy: IntelliJDummy,
        ev0: LocalPeer[P],
        ev1: T <:< (R <=> U)): R => U = ???

  @annotation.compileTimeOnly("Used to guide IntelliJ IDEA Scala Plugin type inference. Do not use directly.")
  final implicit def $$retier$valuePlacedIssued[P <: Peer, T, U]
    (v: T localOn P)
    (implicit
        dummy: IntelliJDummy,
        ev0: LocalPeer[P],
        ev1: T <:< (_ <-> U)): U = ???
}