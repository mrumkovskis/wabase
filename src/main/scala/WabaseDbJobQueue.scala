package org.wabase

import org.tresql._
import org.wabase.ds.PoolName

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, Serializable}
import java.sql.SQLException
import scala.annotation.{nowarn, tailrec}
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Waiting runs of one job name, stored in `cron_job_queue` and shared by every node.
  *
  * `cron_job_queue_seq` is a single row (`id` = 1). Its `next_id` allocates queue
  * ids, and updating that row locks queue changes so two nodes cannot insert the
  * same run or take the same row. Pool is `app.job.job-status-cp` when created
  * from [[WabaseDbJobQueueFactory]].
  *
  * The run in progress is not stored here. At most `maxSize` waiting runs per job
  * name. Equal parameters (the bytes from [[encodeParams]]) are kept once. `poll` removes the
  * oldest row. [[offerFront]] puts a run back ahead of the others and does not
  * apply `maxSize` — a run that was taken and could not be started must not be
  * dropped. A `maxSize` of zero or less stores nothing.
  *
  * Use with [[DefaultWabaseJobStatusController]] (or another controller that
  * takes the running lock). The queue does not itself stop two nodes from running
  * the same job name.
  */
class WabaseDbJobQueue(
  val dbAccess: DbAccess,
  val pool: PoolName,
  val jobName: String,
  maxSize: Int,
) extends WabaseJobQueue(maxSize) with Loggable {

  override def loggerName: String = "wabase.job-queue"

  /** Bytes stored in `cron_job_queue.params`. Object keys are sorted, so map iteration
    * order does not change the bytes. Values are written with [[java.io.ObjectOutputStream ObjectOutputStream]],
    * the same way deferred requests are stored. A subclass that overrides this must
    * override [[decodeParams]] with the matching reader. Values must be
    * [[java.io.Serializable]]. Map keys must be strings.
    */
  protected def encodeParams(params: Map[String, Any]): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    try out.writeObject(canonicalize(params).asInstanceOf[Serializable])
    finally out.close()
    bos.toByteArray
  }

  /** Reads bytes from [[encodeParams]] with [[java.io.ObjectInputStream ObjectInputStream]]. */
  protected def decodeParams(encoded: Array[Byte]): Map[String, Any] = {
    val in = new ObjectInputStream(new ByteArrayInputStream(encoded))
    try in.readObject.asInstanceOf[Map[String, Any]]
    finally in.close()
  }

  /** Maps become [[scala.collection.immutable.ListMap ListMap]]s with sorted string keys so serialization does not
    * depend on map iteration order. Map keys must be strings.
    */
  protected def canonicalize(value: Any): Any = value match {
    case m: scala.collection.Map[_, _] => sortedMap(m)
    case m: java.util.Map[_, _] => canonicalize(m.asScala.toMap)
    case a: Array[Byte] => a
    case a: Array[_] => a.iterator.map(canonicalize).toVector
    case s: Seq[_] => s.map(canonicalize)
    case i: java.lang.Iterable[_] =>
      val items = Vector.newBuilder[Any]
      val it = i.iterator
      while (it.hasNext) items += canonicalize(it.next())
      items.result()
    case other => other
  }

  private def sortedMap(m: scala.collection.Map[_, _]): ListMap[String, Any] = {
    val fields = m.iterator.map { case (k, v) =>
      val key = k match {
        case s: String => s
        case null => throw new IllegalArgumentException("Job parameter map key is null")
        case other =>
          throw new IllegalArgumentException(
            s"Job parameter map key must be a string, got ${other.getClass.getName}")
      }
      key -> canonicalize(v)
    }.toSeq.sortBy(_._1)
    ListMap(fields: _*)
  }

  private def transaction[A](f: Resources => A): A =
    dbAccess.newTransaction(
      poolName = pool,
      template = dbAccess.withDbAccessLogger(
        dbAccess.tresqlResources.resourcesTemplate,
        loggerName)
    )(f)

  /** Serialize queue changes on the single `cron_job_queue_seq` row.
    * Retries once when two nodes create that row at the same time.
    */
  private def locked[A](f: Resources => A): A =
    try runLocked(f) catch {
      case NonFatal(e) if duplicateKey(e) => runLocked(f)
    }

  private def runLocked[A](f: Resources => A): A = transaction { implicit res =>
    Query(
      """+cron_job_queue_seq
        |{id, next_id}
        |{1, 0}
        |[!(cron_job_queue_seq existing[id = 1])]""".stripMargin)
    Query("=cron_job_queue_seq[id = 1]{next_id}[next_id]")
    f(res)
  }

  private def duplicateKey(e: Throwable): Boolean = {
    var current: Throwable = e
    while (current != null) {
      current match {
        case sql: SQLException if sql.getSQLState != null && sql.getSQLState.startsWith("23") =>
          return true
        case _ =>
      }
      current = current.getCause
    }
    false
  }

  @nowarn("msg=Manifest")
  override def size: Int =
    if (maxSize <= 0) 0
    else transaction { implicit res =>
      Query.unique[Int]("cron_job_queue[job_name = ?]{count(1)}", jobName)
    }

  @nowarn("msg=Manifest")
  override def offer(params: Map[String, Any]): WabaseJobQueue.Result =
    if (maxSize <= 0) WabaseJobQueue.Disabled
    else {
      val encoded = encodeParams(params)
      locked { implicit res =>
        if (Query.unique[Int](
          "cron_job_queue[job_name = ? & params = ?]{count(1)}", jobName, encoded) > 0)
          WabaseJobQueue.Duplicate
        else if (Query.unique[Int]("cron_job_queue[job_name = ?]{count(1)}", jobName) >= maxSize)
          WabaseJobQueue.Full
        else {
          insert(allocateId(), encoded)
          WabaseJobQueue.Queued
        }
      }
    }

  @nowarn("msg=Manifest")
  override def offerFront(params: Map[String, Any]): Unit =
    if (maxSize > 0) {
      val encoded = encodeParams(params)
      locked { implicit res =>
        if (Query.unique[Int](
          "cron_job_queue[job_name = ? & params = ?]{count(1)}", jobName, encoded) == 0) {
          val id = Query.unique[java.lang.Long]("cron_job_queue{min(id)}") match {
            case null => allocateId()
            case minId => minId.longValue - 1
          }
          insert(id, encoded)
        }
      }
    }

  @nowarn("msg=Manifest")
  override def poll(): Option[Map[String, Any]] =
    if (maxSize <= 0) None
    else locked { implicit res =>
      @tailrec def claim(): Option[Array[Byte]] =
        Query.headOption[Long, Array[Byte]](
          "cron_job_queue[job_name = ?]{id, params}#(id)@(1)", jobName
        ) match {
          case None => None
          case Some((id, params)) =>
            if (Query("-cron_job_queue[id = ?]", id).affectedRowCount > 0) Some(params)
            else claim()
        }
      claim().map(decodeParams)
    }

  /** Job names that have a waiting run. */
  @nowarn("msg=Manifest")
  def jobNames: Seq[String] = transaction { implicit res =>
    Query.list[String]("cron_job_queue{job_name}(job_name)")
  }

  @nowarn("msg=Manifest")
  private def allocateId()(implicit res: Resources): Long = {
    Query("=cron_job_queue_seq[id = 1]{next_id}[next_id + 1]")
    Query.unique[Long]("cron_job_queue_seq[id = 1]{next_id}")
  }

  private def insert(id: Long, encoded: Array[Byte])(implicit res: Resources): Unit =
    Query(
      """+cron_job_queue
        |{id, job_name, params, enqueued_time}
        |{?, ?, ?, now()}""".stripMargin,
      id, jobName, encoded)
}

/** Database-backed job queue shared by all nodes that use this pool.
  *
  * This is the default `app.job.queue`. `app.job.queue-size` must be above zero
  * or waiting runs are not stored. Create `cron_job_queue` (`id` long primary
  * key, `job_name` string 60, `params` bytes, `enqueued_time` dateTime) and
  * `cron_job_queue_seq` (`id` int primary key, `next_id` long). Both use
  * `app.job.job-status-cp`. Use [[org.wabase.DefaultWabaseJobStatusController]]
  * so the same job name does not run on two nodes. `app.job.clean-jobs-on-start`
  * does not delete queued runs.
  *
  * {{{
  * app.job.queue-size = 10
  * app.job.status-controller = org.wabase.DefaultWabaseJobStatusController
  * }}}
  */
class WabaseDbJobQueueFactory(val dbAccess: DbAccess) extends WabaseJobQueueFactory {
  val pool: PoolName = PoolName(config.getString("app.job.job-status-cp"))

  def create(jobName: String, maxSize: Int): WabaseJobQueue =
    new WabaseDbJobQueue(dbAccess, pool, jobName, maxSize)

  override def acceptsWhenLockHeldElsewhere: Boolean = true

  override def pendingJobNames: Seq[String] =
    new WabaseDbJobQueue(dbAccess, pool, "", 0).jobNames
}
