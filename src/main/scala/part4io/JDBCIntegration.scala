package part4io

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._

import java.sql.PreparedStatement

object JDBCIntegration {

  /*
  commands for terminal and postgres:
  with postgres initialized in docker

  docker exec -it rockthejvm-flink-postgres bash

  psql -U docker

  create database rtjvm;
  \c rtjvm;
  create table people(name varchar(40) not null, age int not null);
  select * from people;
   */

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  case class Person(name: String, age: Int)

  // write data to JDBC
  def demoWriteToJDBC(): Unit = {
    val people = env.fromElements(
      Person("Abby", 99),
      Person("Alice", 1),
      Person("Bob", 10),
      Person("Mary Jane", 43)
    )

    val jdbcSink = JdbcSink.sink[Person](
      // 1 - SQL statement
      "insert into people (name, age) values (?, ?)",
      new JdbcStatementBuilder[Person] { // the way to expand the wildcards with actual values
        override def accept(statement: PreparedStatement, person: Person): Unit = {
          statement.setString(1, person.name) // the first ? is replaced with person.name
          statement.setInt(2, person.age)     // similar
        }
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:postgresql://localhost:5432/rtjvm")
        .withDriverName("org.postgresql.Driver")
        .withUsername("docker")
        .withPassword("docker")
        .build()
    )

    // push the data through the sink
    people.addSink(jdbcSink)
    people.print()
    env.execute()
  }

  def main(args: Array[String]): Unit =
    demoWriteToJDBC()
}
