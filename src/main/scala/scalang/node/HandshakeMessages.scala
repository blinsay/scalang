package scalang.node

case object ConnectedMessage

case class NameMessage(version : Short, flags : Int, name : String)

case class StatusMessage(status : String)

case class ChallengeMessage(version : Short, flags : Int, challenge : Int, name : String)

case class ChallengeReplyMessage(challenge : Int, digest : Array[Byte])

case class ChallengeAckMessage(digest : Array[Byte])

case object HandshakeSucceeded

case object HandshakeFailed

object DistributionFlags {
  val published = 1
  val atomCache = 2
  val extendedReferences = 4
  val distMonitor = 8
  val funTags = 0x10
  val distMonitorName = 0x20
  val hiddenAtomCache = 0x40
  val newFunTags = 0x80
  val extendedPidsPorts = 0x100
  val exportPtrTag = 0x200
  val bitBinaries = 0x400
  val newFloats = 0x800
  
  val default = extendedReferences | extendedPidsPorts |
    bitBinaries | newFloats | funTags | newFunTags
}

class ErlangAuthException(msg : String) extends Exception(msg)