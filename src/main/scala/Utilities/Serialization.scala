package Utilities

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

object Serialization {
  def SerializeObject(o: Object): String = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(o)
    oos.close()
    new String(
      Base64.getEncoder().encode(stream.toByteArray),
      StandardCharsets.UTF_8
    )
  }
}
