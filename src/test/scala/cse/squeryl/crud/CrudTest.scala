package cse.squeryl.crud

import java.sql.{Connection, DriverManager}
import org.scalatest.path
import org.squeryl.PrimitiveTypeMode._
import org.squeryl.adapters.H2Adapter
import org.squeryl.{Schema, Session, SessionFactory}

/**
 * Created by dnwiebe on 10/7/15.
 */

case class Item (id: Int, name: String, price: Int)

object CrudSchema extends Schema {
  val items = table[Item]
}

class CrudTest extends path.FunSpec {

  private def makeTables: Connection = {
    Class.forName ("org.h2.Driver")
    val conn = DriverManager.getConnection ("jdbc:h2:mem:test")
    val stmt = conn.createStatement ()
    stmt.execute (
      """
        | create table item (
        |   id integer primary key,
        |   name varchar (40),
        |   price integer
        | )
      """.stripMargin
    )
    stmt.close ()
    conn
  }

  describe ("A database with one table") {
    val conn = makeTables

    describe ("with a SessionFactory pointed at it") {
      SessionFactory.concreteFactory = Some (
        () => Session.create (java.sql.DriverManager.getConnection ("jdbc:h2:mem:test"), new H2Adapter ())
      )

      describe ("and a row added to it") {
        transaction {
          CrudSchema.items.insert (new Item (432, "Snickers bar", 79))
        }

        describe ("when asked for that row") {
          transaction {
            val result = from (CrudSchema.items) {r => where (r.id === 432).select (r)}

            it ("returns the correct value") {
              assert (result.head === new Item (432, "Snickers bar", 79))
            }
          }
        }

        describe ("and the row changed") {
          transaction {
            update (CrudSchema.items) {r => where (r.id === 432).set (r.price := 99)}
          }

          describe ("when asked for that row") {
            val result = transaction {
              from (CrudSchema.items) {r => where (r.id === 432).select (r)}.head
            }

            it ("returns the changed value") {
              assert (result === new Item (432, "Snickers bar", 99))
            }
          }
        }

        describe ("and the row deleted") {
          transaction {
            CrudSchema.items.deleteWhere {r => r.id === 432}
          }

          describe ("when asked for that row") {
            val result = transaction {
              from (CrudSchema.items) {r => where (r.id === 432).select (r)}.isEmpty
            }

            it ("returns no value") {
              assert (result)
            }
          }
        }
      }
    }

    conn.close ()
  }
}
