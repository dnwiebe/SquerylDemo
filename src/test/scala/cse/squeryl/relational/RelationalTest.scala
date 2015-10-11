package cse.squeryl.relational

import java.sql.{DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util.Date

import org.scalatest.path
import org.squeryl.PrimitiveTypeMode._
import org.squeryl.adapters.H2Adapter
import org.squeryl.{Schema, Session, SessionFactory}

/**
 * Created by dnwiebe on 10/7/15.
 */

case class Item (id: Int, name: String, price: Long)
case class Customer (id: Int, name: String)
case class Purchase (id: Int, customerId: Int, date: Date, comment: Option[String]) {
  def this () = this (0, 0, new Date (0L), Some (""))
}
case class LineItem (id: Int, purchaseId: Int, itemId: Int, count: Int)
case class Inventory (id: Int, itemId: Int, date: Date, count: Int)

object History extends Schema {
  val items = table[Item]
  val customers = table[Customer]
  val purchases = table[Purchase]
  val lineItems = table[LineItem]
  val inventories = table[Inventory]
}

class RelationalTest extends path.FunSpec {
  import History._
  
  val sdf = new SimpleDateFormat ("MM-dd-yyyy")

  private def makeTables (conn: Connection) = {
    executeSql (conn,
      """
        | create table item (
        |   id integer primary key,
        |   name varchar (40),
        |   price long
        | )
      """.stripMargin
    )
    executeSql (conn,
      """
        | create table customer (
        |   id integer primary key,
        |   name varchar (40)
        | )
      """.stripMargin
    )
    executeSql (conn, // notice nullable column
      """
        | create table purchase (
        |   id integer primary key,
        |   customerId integer,
        |   date date,
        |   comment varchar (1000) null
        | )
      """.stripMargin
    )
    executeSql (conn,
      """
        | create table lineItem (
        |   id integer primary key,
        |   purchaseId integer,
        |   itemId integer,
        |   count integer
        | )
      """.stripMargin
    )
    executeSql (conn,
      """
        | create table inventory (
        |   id integer primary key,
        |   itemId integer,
        |   date date,
        |   count integer
        | )
      """.stripMargin
    )
    conn
  }

  describe ("A database with several tables") {
    Class.forName ("org.h2.Driver")
    val backgroundConn = DriverManager.getConnection ("jdbc:h2:mem:test")
    makeTables (backgroundConn)

    describe ("with a SessionFactory pointed at it") {
      SessionFactory.concreteFactory = Some (
        () => Session.create (DriverManager.getConnection ("jdbc:h2:mem:test"), new H2Adapter ())
      )

      describe ("filled with some sample data") {
        transaction {
          items.insert (new Item (101, "Snickers bar", 99))
          items.insert (new Item (102, "Can of peanuts", 399))
          items.insert (new Item (103, "Aircraft carrier", 450000000000L))

          customers.insert (new Customer (301, "Mindy"))
          customers.insert (new Customer (302, "Billy"))

          inventories.insert (new Inventory (201, 101, sdf.parse ("01-01-2014"), 547))
          inventories.insert (new Inventory (202, 102, sdf.parse ("01-01-2014"), 12))
          inventories.insert (new Inventory (203, 103, sdf.parse ("01-01-2014"), 8))

          purchases.insert (new Purchase (401, 301, sdf.parse ("03-11-2014"), None))
          lineItems.insert (new LineItem (501, 401, 101, 14))
          lineItems.insert (new LineItem (502, 401, 102, 1))
          purchases.insert (new Purchase (402, 301, sdf.parse ("03-18-2014"), None))
          lineItems.insert (new LineItem (503, 402, 101, 14))
          lineItems.insert (new LineItem (504, 402, 102, 1))
          purchases.insert (new Purchase (403, 301, sdf.parse ("03-25-2014"), None))
          lineItems.insert (new LineItem (505, 403, 101, 14))
          lineItems.insert (new LineItem (506, 403, 102, 1))

          purchases.insert (new Purchase (404, 302, sdf.parse ("04-01-2014"), Some ("Johnny took real good care of us")))
          lineItems.insert (new LineItem (507, 404, 103, 2))

          inventories.insert (new Inventory (204, 101, sdf.parse ("01-01-2015"), 510))
          inventories.insert (new Inventory (205, 102, sdf.parse ("01-01-2015"), 9))
          inventories.insert (new Inventory (206, 103, sdf.parse ("01-01-2015"), 5))
        }

        describe ("asked for the number of orders from each customer") {
          val result = transaction {
            from (purchases, customers) {(p, c) =>
              where (c.id === p.customerId)
                .groupBy (c.name) // Notice not c.id
                .compute (count (p.id))
                .orderBy (c.name.desc)
            }.map {f => (f.key, f.measures)} // Notice property names
          }

          it ("returns 3 for Mindy and 1 for Billy") {
            assert (result === List(("Mindy", 3), ("Billy", 1)))
          }
        }

        describe ("asked for the total revenue from each customer") {
          val result = transaction {
            from (customers, lineItems, purchases, items) { (c, l, p, i) =>
              where (
                (i.id === l.itemId)
                  .and (p.id === l.purchaseId)
                  .and (c.id === p.customerId)
              )
                .groupBy (c.name)
                .compute (sum (i.price.times (l.count)))
                .orderBy (c.name.desc)
            }.map (f => (f.key, f.measures))
          }

          it ("returns not much for Mindy but a lot for Billy") {
            assert (result === List (("Mindy", Some (5355)), ("Billy", Some (900000000000L))))
          }
        }

        describe ("asked for the total revenue from each item") {
          val result = transaction {
            from (lineItems, items) { (l, i) =>
              where (i.id === l.itemId)
                .groupBy (i.name)
                .compute (sum (i.price times l.count)) // Notice "times," not "*"
                .orderBy (i.id.asc)
            }.map (f => (f.key, f.measures))
          }

          it ("returns proper values") {
            assert (result === List (
              ("Snickers bar", Some (4158L)),
              ("Can of peanuts", Some (1197L)),
              ("Aircraft carrier", Some (900000000000L))
            ))
          }
        }

        describe ("asked to check inventory") {
          val result = transaction {
            val inventoriesByItem = mapOfLists (from (items, inventories) {(it, in) =>
              where (it.id === in.itemId)
                .select (it.name, in.date, in.count)
                .orderBy (it.name.asc, in.date.desc)
            })

            val purchasesByItem = mapOfLists (from (purchases, items, lineItems) {(p, i, l) =>
              where (
                (i.id === l.itemId)
                .and (p.id === l.purchaseId)
              )
                .select (i.name, p.date, l.count)
                .orderBy (i.name.asc, p.date.desc)
            })

            val itemNames = inventoriesByItem.keys.toList.sorted
            itemNames.flatMap { itemName =>
              makeInventoryReports (purchasesByItem (itemName), inventoriesByItem (itemName)).map { report =>
                val (startDate, endDate, discrepancy) = report
                val strDisc = discrepancy match {
                  case d if d < 0 => s"${d}"
                  case d if d == 0 => "*"
                  case d => s"+${d}"
                }
                s"${itemName} from ${sdf.format (startDate)} to ${sdf.format (endDate)}: ${strDisc}"
              }
            }
          }

          it ("responds appropriately") {
            assert (result === List (
              "Aircraft carrier from 01-01-2014 to 01-01-2015: -1",
              "Can of peanuts from 01-01-2014 to 01-01-2015: *",
              "Snickers bar from 01-01-2014 to 01-01-2015: +5"
            ))
          }
        }
      }
    }

    backgroundConn.close ()
  }

  private def mapOfLists[K, A, B] (seq: Iterable[(K, A, B)]): Map[K, List[(A, B)]] = {
    seq.foldLeft (Map[K, List[(A, B)]]()) {(soFar, elem) =>
      val (k, a, b) = elem
      soFar.contains (k) match {
        case true => soFar + ((k, (a, b) :: soFar(k)))
        case false => soFar + ((k, List ((a, b))))
      }
    }
  }

  private def makeInventoryReports (purchases: List[(Date, Int)], inventories: List[(Date, Int)]): List[(Date, Date, Int)] = {
    inventories.zip (inventories.tail).map {pair =>
      val (firstInv, lastInv) = pair
      val (firstDate, firstCount) = firstInv
      val (lastDate, lastCount) = lastInv
      val actualReduction = firstCount - lastCount
      val expectedReduction = purchases.foldLeft (0) {(soFar, elem) =>
        val (date, count) = elem
        date.after (firstDate) && date.before (lastDate) match {
          case true => soFar + count
          case false => soFar
        }
      }
      (firstDate, lastDate, expectedReduction - actualReduction)
    }
  }

  private def executeSql (conn: Connection, sql: String): Unit = {
    val stmt = conn.createStatement ()
    stmt.execute (sql)
    stmt.close ()
  }
}
