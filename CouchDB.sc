import ammonite.ops._
val wd = pwd
import $ivy.`org.scalaj::scalaj-http:2.4.1`
import $ivy.`org.json4s::json4s-native:3.6.0-M3`
import $ivy.`org.json4s::json4s-jackson:3.6.0-M3`

import java.io.InputStream
import org.json4s.JValue
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{parse, compact, render}
import scalaj.http.{Http, HttpResponse}
import scala.collection.mutable.ListBuffer

case class CouchDb(database:String, host: String = "localhost", port:Int =5984, protocol: String = "http") {
  def realDatabase: String = database.replace("-", "_")
  def root: String = protocol + "://" + host + ":"  + port + "/" + realDatabase
  def bulkDocs: String = root + "/_bulk_docs"

  def post(url: String, jv: JValue): HttpResponse[String] = {
    val payload: String = compact(render(jv))
    Http(url).header ("Content-Type", "application/json").postData(payload).asString
  }
  def removeDatabase = {
    println(parse( Http(root).header ("Content-Type", "application/json").method("DELETE").asString.body))
  }
  def compactDatabase: Unit = {
    println(parse( Http(s"${root}/_compact").header("Content-Type", "application/json").method("POST").asString.body))
    println(parse( Http(s"${root}/_view_cleanup").header("Content-Type", "application/json").method("POST").asString.body))
  }
  def reIndexUsers: Unit = {
    println(parse(Http(s"${root}/_design/auth/_view/users?limit=1").asString.body))
  }
  def reIndexBackend: Unit = {
    println(parse(Http(s"${root}/_design/backend/_view/orders_recent?limit=1").asString.body))
  }
}

case class Url(part: String) {
  def quote(x:String): String = "\"" + x + "\""
  def encode: String = java.net.URLEncoder.encode(quote(part), "utf-8")
}

trait CouchDbUrlFilter {
  def asUrlPart: String
}
case class CouchDbUrlFilterExact(key: String) extends CouchDbUrlFilter {
  override def asUrlPart: String = "key=" + Url(key).encode
}
case class CouchDbUrlFilterBetween(startKey: Option[String] = None, endKey: Option[String] = None) extends CouchDbUrlFilter {
  override def asUrlPart: String = {
    val params = new ListBuffer[String]
    startKey.map(k => { params += "startkey=" + Url(k).encode })
    endKey.map(k => { params += "endkey=" + Url(k).encode })
    params.mkString("&")
  }
}
case class CouchDbUrlFilterStartingWith(key: String, descending: Boolean = false) extends CouchDbUrlFilter {
  override def asUrlPart: String = {
    if (descending) {
      List("startkey=[" + Url(key).encode + ",{}]", "endkey=[" + Url(key).encode + "]").mkString("&")
    } else {
      List("startkey=[" + Url(key).encode + "]", "endkey=[" + Url(key).encode + ",{}]").mkString("&")
    }
  }
}
case class CouchDbUrlIntFilterStartingWith(key: Int, descending: Boolean = false) extends CouchDbUrlFilter {
  override def asUrlPart: String = {
    if (descending) {
      List("startkey=[" + key + ",{}]", "endkey=[" + key + "]").mkString("&")
    } else {
      List("startkey=[" + key + "]", "endkey=[" + key + ",{}]").mkString("&")
    }
  }
}

case class CouchDbBatch(limit: Int = 500, var skip: Int = 0) {
  def asUrlPart: String = "limit=" + limit + "&skip=" + skip
  def next = { skip += limit }
}
object DefaultCouchDbBatch extends CouchDbBatch()

case class CouchDbUrlBuilder(
                              basePath: String,
                              includeDocs: Boolean = false,
                              reduce: Boolean = false,
                              descending: Option[Boolean] = None,
                              group: Option[String] = None,
                              filter: Option[CouchDbUrlFilter] = None,
                              batch: CouchDbBatch = DefaultCouchDbBatch,
                              stale: Boolean = false
                            ) {
  def getUrl: String = {
    val params = new ListBuffer[String]
    params += "reduce=" + (if (reduce) "true" else "false")
    params += "include_docs=" + (if (includeDocs) "true" else "false")
    group.map(g => { params += "group=" + g })
    filter.map(f => { params += f.asUrlPart })
    descending.map(d => { params += "descending=" + (if (d) "true" else "false") })
    if (stale) { params += "stale=ok"; }
    params += batch.asUrlPart
    basePath + "?" + params.mkString("&")
  }
}

case class CouchDbView(conn: CouchDb, urlBuilder:CouchDbUrlBuilder) {
  def getUrl: String = conn.root + urlBuilder.getUrl
  def get: HttpResponse[String] = Http(getUrl).asString

  def docs(parser: JObject => Unit): Unit = {
    if (urlBuilder.includeDocs) {
      val json: JValue = parse(get.body)
      for (JArray(rows) <- json \ "rows"; JObject(row) <- rows; JField("doc", JObject(doc)) <- row) {
        parser(doc)
      }
    }
  }
  def allDocs[T](parser: (JObject) => T): Any = {
    if (urlBuilder.includeDocs) {
      var canContinue: Boolean = true
      do {
        println("URL: " + getUrl)
        val json: JValue = parse(get.body)
        val JArray(rows) = json \ "rows"
        canContinue = rows.length == urlBuilder.batch.limit
        for {
          JObject(row) <- rows
          JField("doc", JObject(doc)) <- row
        } yield parser(doc)
        urlBuilder.batch.next
      } while (canContinue)
    }
  }

  // this JSON is useful for deleting documents
  def getIdRevDelete: List[JValue] = {
    val json: JValue = parse(get.body)
    for {
      JArray (rows) <- json \ "rows"
      JObject (row) <- rows
      JField ("id", JString (id) ) <- row
      JField ("doc", JObject (doc) ) <- row
      JField ("_rev", JString (rev) ) <- doc
    } yield ("_id" -> id) ~ ("_rev" -> rev) ~ ("_deleted" -> true)
  }
}


case class CouchDbRemover(view: CouchDbView) {
  def get: List[JValue] = { view.getIdRevDelete }
  def submit: HttpResponse[String] =  {
    val jv:JValue = "docs" -> view.getIdRevDelete
    println(jv)
    view.conn.post(view.conn.bulkDocs, jv)
  }
}
