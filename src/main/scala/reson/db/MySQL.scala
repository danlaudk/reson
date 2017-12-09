package reson.db

import com.twitter.conversions.time._
import com.twitter.finagle.client.DefaultPool
import com.twitter.finagle.exp.Mysql

import scala.util.Try
//import com.twitter.finagle.postgres._
//import com.twitter.finagle.postgres.values._

import com.twitter.finagle.exp.mysql.OK
import com.twitter.finagle.exp.mysql._
import com.twitter.util.Future
import rapture.json._
import rapture.json.jsonBackends.jackson._

/**
  * Created by sscarduzio on 23/12/2015.
  */

object MySQL extends MySQL2Json {
    case class Relation(relSchema: String
        , relTable   : String
        , relColumn  : String
        , relFTable : String
        , relFColumn : String
        , relType    : String)
  val dbConf = ConnectionConfig(sys.props.get("db_uri").getOrElse(sys.env("db_uri")))
  println(sys.props.get("db_uri").getOrElse(sys.env("db_uri")) )
  lazy val db = Mysql.client
    .withCredentials(dbConf.user, dbConf.pass.getOrElse(null))
    .withDatabase(dbConf.dbName)
    .configured(DefaultPool.Param(
      low = 1, high = 10,
      idleTime = 5.minutes,
      bufferSize = 0,
      maxWaiters = Int.MaxValue))
    .newRichClient(dbConf.hostAndPort.toString)
  lazy val dbStructure:Try[String] = Try {
    s"""SELECT DISTINCT
              tc.table_schema, tc.table_name, kcu.column_name,
              kcu.referenced_table_name AS foreign_table_name,
              kcu.referenced_column_name AS foreign_column_name
          FROM information_schema.table_constraints AS tc
          JOIN information_schema.key_column_usage AS kcu on tc.constraint_name = kcu.constraint_name and
          tc.table_schema=kcu.table_schema and tc.table_name=kcu.table_name
          WHERE   tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema NOT IN ('information_schema','mysql','performance_schema')
          ORDER BY tc.table_schema, tc.table_name, kcu.column_name"""
  }
  def tableList: Future[String] = {
    // Shitty bug in MySQL derived column return type
    def fixBigintToBool(j: Json): Json = {
      val isInsertable =  j.insertable.as[Int] == 1
      j ++ json"""{"insertable": $isInsertable}"""
    }
    val jbListF: Future[Seq[Json]] =
      db.prepare(
        """SELECT TABLE_SCHEMA as `schema`, TABLE_NAME as name, TABLE_TYPE = 'BASE TABLE' as insertable
          |FROM information_schema.tables
          |WHERE TABLE_SCHEMA = ? """.stripMargin)
        .select(dbConf.dbName)(Json(_)) // dbName is fed into sql statement as param "?"
        .map(_.map(fixBigintToBool))
    jbListF.map(lj => Json(lj).toString)
  }

  def read(query: String): Future[String] = {
    println(s"READ: $query")
    // structure.toString. put here bc this is mysql specific (not finagle etc
    db.select(query)(Json(_)).map(q => Json(q).toString)
  }

//  def dbaStructure() = Try {
//      def addRelations():List[Relations] = {
//           val query = """SELECT DISTINCT
//              tc.table_schema, tc.table_name, kcu.column_name,
//              kcu.referenced_table_name AS foreign_table_name,
//              kcu.referenced_column_name AS foreign_column_name
//          FROM information_schema.table_constraints AS tc
//          JOIN information_schema.key_column_usage AS kcu on tc.constraint_name = kcu.constraint_name and
//          tc.table_schema=kcu.table_schema and tc.table_name=kcu.table_name
//          WHERE   tc.constraint_type = 'FOREIGN KEY'
//              AND tc.table_schema NOT IN ('information_schema','mysql','performance_schema')
//          ORDER BY tc.table_schema, tc.table_name, kcu.column_name"""
//
//          val resultList = db.select(query) { rowWillGoToTuple =>
//            val StringValue(s) = rowWillGoToTuple("tc.table_schema").get
//            val StringValue(t) = rowWillGoToTuple("tc.table_name").get
//            val StringValue(c) = rowWillGoToTuple("kcu.column_name").get
//            val StringValue(ft) = rowWillGoToTuple("foreign_table_name").get
//            val StringValue(fc) = rowWillGoToTuple("foreign_column_name").get
//            (s,t,c,ft,fc)
//          }
//          //def relationFromRow() = Relation(s,t,c,ft,fc)
//          def addFlippedRelation(row:Tuple5[String,String,String,String,String],list:List[Relation]): List[Relation]= {
//              row match { case (s,t,c,ft,fc) =>
//                {
//                  Relation(s,t,c,ft,fc,"parent") :: Relation(s,t,c,ft,fc,"child") :: list
//                }
//              }
//          }
//          resultList.foldRight (List())(addFlippedRelation)
//      }
//      ???
//  }

  def write(query: String): Future[String] = {
    println(s"WRITE: $query")
    db.query(query).flatMap {
      _ match {
        case r: OK => Future.value(json"""{ "status": "OK","affected_rows": ${r.affectedRows}}""".toString)
        case err => Future.exception(new Exception(err.toString))
      }
    }
  }

}
