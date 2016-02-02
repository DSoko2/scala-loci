package retier
package impl

import RemoteRef._
import network.ProtocolInfo
import util.Notifier
import java.util.concurrent.atomic.AtomicBoolean

private final case class RemoteRefImpl(peerType: PeerType, id: Long,
    protocol: ProtocolInfo)(
    val connected: AtomicBoolean,
    val authenticated: AtomicBoolean) extends Remote[Nothing] {
  val doDisconnected = Notifier[Unit]

  def disconnected = doDisconnected.notification
  def isConnected = connected.get
  def isAuthenticated = authenticated.get
  def authenticate() = authenticated set true
}

object RemoteRef {
  type RemoteRef = transmission.RemoteRef

  private[impl] def create[R <: Peer: PeerTypeTag](id: Long,
      protocol: ProtocolInfo): Remote[R] =
    RemoteRefImpl(
      peerTypeOf[R], id, protocol)(
      new AtomicBoolean(true), new AtomicBoolean(protocol.isAuthenticated))

  private[impl] def create(peerType: PeerType, id: Long,
      protocol: ProtocolInfo): RemoteRef =
    RemoteRefImpl(
      peerType, id, protocol)(
      new AtomicBoolean(true), new AtomicBoolean(protocol.isAuthenticated))

  implicit class RemoteRefOps(remote: RemoteRef) {
    def peerType: PeerType = remote match {
      case RemoteRefImpl(peerType, _, _) => peerType
      case _ => throwRetierImplementationError(remote)
    }

    def id: Long = remote match {
      case RemoteRefImpl(_, id, _) => id
      case _ => throwRetierImplementationError(remote)
    }

    def disconnect(): Unit = remote match {
      case remote @ RemoteRefImpl(_, _, _) =>
        remote.connected set false
        remote.doDisconnected()
      case _ => throwRetierImplementationError(remote)
    }

    def asRemote[R <: Peer: PeerTypeTag]: Option[Remote[R]] = remote match {
      case remote @ RemoteRefImpl(tpe, _, _) =>
        if (tpe <= peerTypeOf[R]) Some(remote)
        else None
      case _ => throwRetierImplementationError(remote)
    }

    def asRemote(peerType: PeerType): Option[RemoteRef] = remote match {
      case remote @ RemoteRefImpl(tpe, _, _) =>
        if (tpe <= peerType) Some(remote)
        else None
      case _ => throwRetierImplementationError(remote)
    }

    private def throwRetierImplementationError(ref: Any) =
      throw new RetierImplementationError(
        s"invalid remote reference implementation: ${className(ref)}")
  }
}