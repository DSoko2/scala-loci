package loci
package communicator
package tcp

import scala.util.Success
import scala.util.Failure
import scala.util.control.NonFatal
import java.net.{ConnectException, Socket}

private class TCPConnector(
  host: String, port: Int, properties: TCP.Properties)
    extends Connector[TCP] {

  protected def connect(connectionEstablished: Connected[TCP]): Unit = {
    new Thread() {
      var retryCounter = 5
      val backoffMs = 1000L

      override def run =
        try TCPHandler handleConnection (
          new Socket(host, port), properties, TCPConnector.this, { connection =>
            connectionEstablished set Success(connection)
          })
        catch {
          case exception: ConnectException  =>
            println(s"Connection failed, retry in $backoffMs ms")
            if (retryCounter > 0) {
              retryCounter -= 1
              Thread.sleep(backoffMs)
              run
            }
            else
              connectionEstablished set Failure(exception)
          case NonFatal(exception) =>
            connectionEstablished set Failure(exception)
        }
    }.start    
  }
}
