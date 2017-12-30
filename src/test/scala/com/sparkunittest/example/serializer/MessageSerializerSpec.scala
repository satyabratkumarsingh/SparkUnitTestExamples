package com.sparkunittest.example.serializer
import com.sparkunittest.example.models.Message
import org.scalatest.FunSpec

class MessageSerializerSpec extends FunSpec {

  it("tests message serialization / deserialization") {

    val message = Message("1", "Message Body")
    val serializedMessage  = MessageSerializer.serialize(message)
    val deserializedMessage = MessageSerializer.deserialize(serializedMessage).asInstanceOf[Message]
    assert(deserializedMessage.messageBody == "Message Body")

  }
}
