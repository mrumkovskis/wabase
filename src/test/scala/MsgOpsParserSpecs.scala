package org.wabase

import io.bullet.borer.Cbor
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.AppMetadata.Action
import org.wabase.AppMetadata.Action.{Db, Evaluation, Foreach, MsgAdmin, MsgRead, MsgSend, Op, Template, Tresql}

class MsgOpsParserSpecs extends FlatSpec with Matchers {
  // table metadata - invocation operation (tried on backtracking) checks table names
  private val parser = new OpParser("msg_test",
    new TestQuerease("/querease-action-specs-metadata.yaml").tableMetadata, getClass.getClassLoader)
  private def op(s: String): Op = parser.parseOperation(s)
  // compare ignoring whitespace
  private def n(t: Tresql): String = if (t == null) null else t.tresql.replaceAll("\\s+", "")
  private def n(s: String): String = s.replaceAll("\\s+", "")

  behavior of "msg read"

  it should "parse all parameters" in {
    op("""msg read [kafka-2] group='g' from={'t' topic, 0 partition, 5 offset} to=:value = 'end'
         |  timeout={'10s' within, '3s' idle} committer={1 max_batch} commit_metadata=:value
         |  key='id' using={'string' key, 'avro' value} restart={'1s' min_backoff, 3 max_restarts}
         |  at_most_once=false""".stripMargin) match {
      case r: MsgRead =>
        r.providerName      shouldBe "kafka-2"
        n(r.group)          shouldBe n("'g'")
        n(r.from)           shouldBe n("null{'t' topic, 0 partition, 5 offset}")
        n(r.to)             shouldBe n(":value = 'end'")
        n(r.timeout)        shouldBe n("null{'10s' within, '3s' idle}")
        n(r.committer)      shouldBe n("null{1 max_batch}")
        n(r.commitMetadata) shouldBe n(":value")
        n(r.key)            shouldBe n("'id'")
        n(r.using)          shouldBe n("null{'string' key, 'avro' value}")
        n(r.restart)        shouldBe n("null{'1s' min_backoff, 3 max_restarts}")
        n(r.atMostOnce)     shouldBe n("false")
      case x => fail(s"Unexpected operation: $x")
    }
  }

  it should "parse minimal operation, union of subscription rows and parenthesized arguments" in {
    op("msg read from={'t1' topic} ++ {'t2' topic}") match {
      case r: MsgRead =>
        n(r.from) shouldBe n("null{'t1' topic} ++ null{'t2' topic}")
        r shouldBe MsgRead(from = r.from)
      case x => fail(s"Unexpected operation: $x")
    }
    op("msg read from=({'t' topic}) to=(:x = 1)") match {
      case r: MsgRead =>
        n(r.from) shouldBe n("null{'t' topic}")
        n(r.to)   shouldBe n(":x = 1")
      case x => fail(s"Unexpected operation: $x")
    }
    // from as positional argument
    op("msg read [p] {'t' topic} to=(:x = 1)") match {
      case r: MsgRead => (r.providerName, n(r.from), n(r.to)) shouldBe (("p", n("null{'t' topic}"), n(":x = 1")))
      case x => fail(s"Unexpected operation: $x")
    }
    // followed by operation
    op("foreach msg read from={'t' topic} timeout={'1s' idle} (:value)") match {
      case f: Foreach =>
        n(f.initOp.asInstanceOf[MsgRead].timeout) shouldBe n("null{'1s' idle}")
        f.action.steps.map(_._1) shouldBe List(Evaluation(None, Nil, Tresql(":value")))
      case x => fail(s"Unexpected operation: $x")
    }
    op("msg read using='avro' from={'t' topic}") match {
      case r: MsgRead => (n(r.using), n(r.from)) shouldBe ((n("'avro'"), n("null{'t' topic}")))
      case x => fail(s"Unexpected operation: $x")
    }
  }

  it should "report invalid parameters" in {
    intercept[RuntimeException](op("msg read group='g'"))
      .getMessage should include ("Missing mandatory parameters (from)")
    intercept[RuntimeException](op("msg read from={'t' topic} from={'u' topic}"))
      .getMessage should include ("Duplicate parameters (from)")
    intercept[RuntimeException](op("msg read from={'t' topic} unknown=1"))
      .getMessage should include ("Illegal argument name - unknown")
    intercept[RuntimeException](op("msg read from={'t' topic} committer={1 max_batch} at_most_once=true"))
      .getMessage should include ("'committer' and 'at_most_once' are mutually exclusive")
    // value parse error of named argument is reported, not hidden by ending arguments
    intercept[RuntimeException](op("msg read group='g' from=:in"))
      .getMessage should include ("'in' is one of the reserved words")
    // dynamic at_most_once is checked at runtime
    op("msg read from={'t' topic} committer={1 max_batch} at_most_once=:amo") shouldBe a [MsgRead]
  }

  behavior of "msg send"

  it should "parse parameters and operation" in {
    op("msg send [p] to={'t' topic, 1 partition} key={'id' name, :id value, true should_remove} " +
       "headers=({'h1' name, 'v1' value} ++ {'h2' name, :v value}) using='cbor' (:data)") match {
      case s: MsgSend =>
        s.providerName shouldBe "p"
        n(s.to)        shouldBe n("null{'t' topic, 1 partition}")
        n(s.key)       shouldBe n("null{'id' name, :id value, true should_remove}")
        n(s.headers)   shouldBe n("null{'h1' name, 'v1' value} ++ null{'h2' name, :v value}")
        n(s.using)     shouldBe n("'cbor'")
        s.action.steps.map(_._1) shouldBe List(Evaluation(None, Nil, Tresql(":data")))
      case x => fail(s"Unexpected operation: $x")
    }
  }

  it should "parse other operations as message source" in {
    def sendAction(s: String) = op(s).asInstanceOf[MsgSend].action.steps.head._1.asInstanceOf[Evaluation].op
    sendAction("msg send to=({'t' topic}) msg read from={'s' topic}") shouldBe a [MsgRead]
    sendAction("msg send to=({'t' topic}) foreach :items (:x)") shouldBe a [Foreach]
    sendAction("msg send to=({'t' topic}) transaction 'x'") shouldBe a [Db]
    sendAction("msg send to=({'t' topic}) 'single message'") shouldBe Tresql("'single message'")
  }

  it should "parse to as positional argument" in {
    def send(s: String) = op(s) match {
      case m: MsgSend => (n(m.to), n(m.key), m.action.steps.map(_._1))
      case x => fail(s"Unexpected operation: $x")
    }
    send("msg send ({'t' topic}) {'x' a}") shouldBe
      ((n("null{'t' topic}"), null, List(Evaluation(None, Nil, Tresql("null{'x' a}")))))
    send("msg send {'t' topic} key=({'id' name}) :data") shouldBe
      ((n("null{'t' topic}"), n("null{'id' name}"), List(Evaluation(None, Nil, Tresql(":data")))))
    // named parameters do not take positional argument index - operation follows
    send("msg send key=({'id' name}) to=({'t' topic}) :data") shouldBe
      ((n("null{'t' topic}"), n("null{'id' name}"), List(Evaluation(None, Nil, Tresql(":data")))))
  }

  it should "reject illegal parameter name instead of parsing it as operation" in {
    intercept[RuntimeException](op("msg send to=({'t' topic}) tpo=1 :data"))
      .getMessage should include ("Illegal argument name - tpo")
  }

  it should "require to and operation" in {
    intercept[RuntimeException](op("msg send key={'k' name} (:x)"))
      .getMessage should include ("Missing mandatory parameters (to)")
    intercept[RuntimeException](op("msg send to=({'t' topic})"))
  }

  behavior of "msg metadata calls"

  it should "parse commands" in {
    op("msg topics") shouldBe MsgAdmin(MsgAdmin.Topics)
    op("msg topics [p]") shouldBe MsgAdmin(MsgAdmin.Topics, "p")
    def admin(s: String) = op(s) match {
      case a: MsgAdmin => (a.command, a.providerName, n(a.args))
      case x => fail(s"Unexpected operation: $x")
    }
    admin("msg topic  partitions {'t' topic}") shouldBe ((MsgAdmin.TopicPartitions, null, n("null{'t' topic}")))
    admin("msg beginning offsets [p] {'t' topic, 0 partition}") shouldBe
      ((MsgAdmin.BeginningOffsets, "p", n("null{'t' topic, 0 partition}")))
    admin("msg end offsets {'t' topic, 0 partition} ++ {'t' topic, 1 partition}") shouldBe
      ((MsgAdmin.EndOffsets, null, n("null{'t' topic, 0 partition} ++ null{'t' topic, 1 partition}")))
    admin("msg committed offsets {'g' group, 't' topic, 0 partition}") shouldBe
      ((MsgAdmin.CommittedOffsets, null, n("null{'g' group, 't' topic, 0 partition}")))
    intercept[RuntimeException](op("msg end offsets"))
  }

  behavior of "operation arguments"

  it should "leave following operation for the next parser" in {
    op("foreach http get (a/b) {'h' h} (:x)") match {
      case f: Foreach =>
        n(f.initOp.asInstanceOf[org.wabase.AppMetadata.Action.Http].headerTresql) shouldBe n("null{'h' h}")
        f.action.steps.map(_._1) shouldBe List(Evaluation(None, Nil, Tresql(":x")))
      case x => fail(s"Unexpected operation: $x")
    }
  }

  it should "leave unknown argument name for enclosing operation" in {
    // nested template with all positional arguments ends at email argument name (like before named argument errors)
    op("email body=template body='B' {1 x} 'target' subject=(template body='S') recipients=({'c' 'to'})") match {
      case e: org.wabase.AppMetadata.Action.Email =>
        (e.subject.asInstanceOf[Template].body, e.body.asInstanceOf[Template].body) shouldBe ((Tresql("'S'"), Tresql("'B'")))
      case x => fail(s"Unexpected operation: $x")
    }
  }

  it should "report misspelled msg operation argument name" in {
    intercept[RuntimeException](op("msg read from='t' grup='g'"))
      .getMessage should include ("Illegal argument name - grup, allowed arguments - from, group, to")
    intercept[RuntimeException](op("msg send to='t' kyy={'id' name} (:x)"))
      .getMessage should include ("Illegal argument name - kyy, allowed arguments - to, key")
    // operation starting with '<ident> =' in braces is msg send body
    op("msg send to='t' (x = 1)") shouldBe a [MsgSend]
  }

  behavior of "msg operations"

  it should "be serializable to action cache" in {
    import CacheIo._
    val action = Action(List(
      Evaluation(None, Nil, op("msg read [p] from={'t' topic} using='avro'")),
      Evaluation(Some("r"), Nil, op("msg send to=({'t' topic}) msg topics")),
      Evaluation(None, Nil, op("msg end offsets {'t' topic, 0 partition}")),
    ).map(_ -> "src"))
    Cbor.decode(Cbor.encode(action).toByteArray).to[Action].value shouldBe action
  }
}
