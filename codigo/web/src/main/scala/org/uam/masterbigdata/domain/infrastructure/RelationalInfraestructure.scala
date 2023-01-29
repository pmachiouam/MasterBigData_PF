package org.uam.masterbigdata.domain.infrastructure

import slick.dbio
import slick.basic.{DatabaseConfig, DatabasePublisher}
import slick.jdbc.JdbcProfile

trait Entity[T, I] {
  val id: Option[I]
}

/** Abstract profile for accessing SQL databases via JDBC. */
trait Profile {
  /*
    Even when using a standard interface for database drivers like JDBC
    there are many differences between databases in the SQL dialect they
    understand, the way they encode data types, or other idiosyncracies.
    Slick abstracts over these differences with profiles.
   */
  val profile: JdbcProfile

}

/**
 * Provides database configuration to the user component
 */
trait DbConfigModule {
  val databaseConfig: DatabaseConfig[JdbcProfile]
}

/**
 * Provides database execution context to the user component.
 * Side effect for pure DBIO's.
 * Interprets the DBIO program sentences.
 */
trait DbModule
  extends Profile {

  import slick.dbio.{DBIO, StreamingDBIO}

  import scala.concurrent.Future

  val db: JdbcProfile#Backend#Database

  implicit def executeOperation[T](dbio: DBIO[T]): Future[T] = {
    db.run(dbio)
  }

  implicit def dbioToDbPub[T](dbio: StreamingDBIO[Seq[T], T]): DatabasePublisher[T] = {
    db.stream(dbio)
  }
}
/*
trait RelationalInfrastructure {
  self: Profile =>

  import profile.api._

  abstract class RelationalTable[T <: Entity[T, Long]](tag: Tag, tableName: String)
    extends Table[T](tag, tableName) {
    // synthetic primary key column:
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
  }

  abstract class RelationalRepository[E <: Entity[E, Long], T <: RelationalTable[E]]
    extends BaseRepository[E] {
    val entities: TableQuery[T]

    override def findAll(): DBIO[Seq[E]] =
      entities.result

    override def streamAll(): StreamingDBIO[Seq[E], E] =
      entities.result

    override def findById(id: Long): DBIO[Option[E]] =
      entities.filter(_.id === id).result.headOption

    override def exists(id: Long): dbio.DBIO[Boolean] = {
      entities.filter(_.id === id).exists.result
    }

    override def count(): DBIO[Int] =
      entities.length.result

    override def insert(e: E): DBIO[Long] =
      entityReturningId() += e

    override def insert(seq: Seq[E]): DBIO[Seq[Long]] =
      entities returning entities.map(_.id) ++= seq

    override def update(e: E): DBIO[Int] =
      entities.filter(_.id === e.id).update(e)

    override def delete(id: Long): DBIO[Int] =
      entities.filter(_.id === id).delete

    override def deleteAll(): DBIO[Int] =
      entities.delete

    /*
      H E L P E R S
     */
    private def entityReturningId(): profile.ReturningInsertActionComposer[E, Long] = entities returning entities.map(_.id)
  }

}
*/