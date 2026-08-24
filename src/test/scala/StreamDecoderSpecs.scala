package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers


class StreamDecoderSpecs extends AsyncFlatSpec with Matchers {
  implicit val system: ActorSystem = ActorSystem("stream-decoder-specs")

  private def decode(name: String, data: String) =
    Source.single(ByteString(data)).via(RequestDecoders.flatStreamDecoders(name)).runWith(Sink.seq)

  behavior of "json stream decoders"

  it should "be created by factory configured for parser" in {
    RequestDecoders.streamDecoderFactories("json")("default_json_decoder") should be (JsonDecoderFactory)
    RequestDecoders.streamDecoderFactories("json")("scalar_json_decoder")  should be (JsonScalarDecoderFactory)
  }

  it should "decode stream of json objects to maps" in {
    decode("default_json_decoder", """[{"a": 1}, {"a": 2}]""").map {
      _ should be (Seq(Map("a" -> 1), Map("a" -> 2)))
    }
  }

  it should "not decode json array of scalars with object framing" in {
    recoverToSucceededIf[Exception] {
      decode("default_json_decoder", """[1, 2, 3]""")
    }
  }

  it should "decode json array of scalars" in {
    decode("scalar_json_decoder", """[1, 2, 3]""").map {
      _ should be (Seq(1, 2, 3))
    }
  }

  it should "decode json array of mixed elements" in {
    decode("scalar_json_decoder", """[1, "a", {"b": 2}, [3, 4]]""").map {
      _ should be (Seq(1, "a", Map("b" -> 2), List(3, 4)))
    }
  }

  it should "decode json array of objects" in {
    decode("scalar_json_decoder", """[{"a": 1}, {"a": 2}]""").map {
      _ should be (Seq(Map("a" -> 1), Map("a" -> 2)))
    }
  }

  it should "refuse json null as array element" in {
    recoverToExceptionIf[BusinessException] {
      decode("scalar_json_decoder", """[1, null]""")
    }.map(_.getMessage should include ("json null is not supported"))
  }

  behavior of "csv and xml stream decoders"

  it should "be created by factory configured for parser" in {
    // parser confs inherit 'factory-class' from format conf, also for parsers configured by application
    RequestDecoders.streamDecoderFactories("csv")("default_csv_decoder") should be (CsvDecoderFactory)
    RequestDecoders.streamDecoderFactories("xml")("test_xml_decoder")    should be (XmlDecoderFactory)
  }

  it should "decode csv stream to maps" in {
    decode("default_csv_decoder", "a,b\n1,2\n").map {
      _ should be (Seq(Map("a" -> "1", "b" -> "2")))
    }
  }

  it should "decode xml stream to maps" in {
    decode("test_xml_decoder", "<data><record><a>1</a></record><record><a>2</a></record></data>").map {
      _ should be (Seq(Map("a" -> "1"), Map("a" -> "2")))
    }
  }

  it should "group decoders by format and keep names unique" in {
    RequestDecoders.streamDecoders.keySet should be (Set("csv", "json", "xml"))
    val names = RequestDecoders.streamDecoders.toSeq.flatMap(_._2.keys)
    names.distinct.size should be (names.size)
    RequestDecoders.flatStreamDecoders.keySet should be (names.toSet)
  }
}
