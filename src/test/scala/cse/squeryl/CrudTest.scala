package cse.squeryl

import java.sql.DriverManager

import org.scalatest.path
import org.squeryl.{Schema, SessionFactory, Session}
import org.squeryl.adapters.H2Adapter
import org.squeryl.PrimitiveTypeMode._

/**
 * Created by dnwiebe on 10/7/15.
 */
class CrudTest extends path.FunSpec {
  describe ("A database with one table") {
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

    describe ("with a SessionFactory pointed at it") {
      SessionFactory.concreteFactory = Some (
        () => Session.create (java.sql.DriverManager.getConnection ("jdbc:h2:mem:test"), new H2Adapter ())
      )

      describe ("and a row added to it") {
        transaction {
          History.items.insert (new Item (432, "Snickers bar", 79))
        }

        describe ("when asked for that row") {
          transaction {
            val result = from (History.items)(r => where (r.id === 432).select (r))

            it ("returns the correct value") {
              assert (result.head === new Item (432, "Snickers bar", 79))
            }
          }
        }

        describe ("and the row changed") {
          transaction {
            update (History.items)(r => where (r.id === 432).set (r.price := 99))
          }

          describe ("when asked for that row") {
            transaction {
              val result = from (History.items)(r => where (r.id === 432).select (r))

              it ("returns the changed value") {
                assert (result.head === new Item (432, "Snickers bar", 99))
              }
            }
          }
        }

        describe ("and the row deleted") {
          transaction {
            History.items.deleteWhere (r => r.id === 432)
          }

          describe ("when asked for that row") {
            transaction {
              val result = from (History.items)(r => where (r.id === 432).select (r))

              it ("returns no value") {
                assert (result.isEmpty)
              }
            }
          }
        }
      }
    }

    conn.close ()
  }
}

case class Item (id: Int, name: String, price: Int) {
  def this () = this (0, "", 0)
}

object History extends Schema {
  val items = table[Item]
}
