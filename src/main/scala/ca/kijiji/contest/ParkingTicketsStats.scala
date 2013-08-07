package ca.kijiji.contest

import ca.kijiji.contest.comparator.ValueComparator
import java.io.InputStream
import java.util._
import org.apache.commons.lang3.StringUtils
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

object ParkingTicketsStats {
  private val StreetSuffixes = Source.fromInputStream(getClass.getResourceAsStream("street_suffixes.txt")).getLines.toSet
  private val Address = """(\d+.*? )?(.+?) ?(E|W|N|S|EAST|WEST|NORTH|SOUTH)?""".r
  private val FineIndex = 4
  private val LocationIndex = 7

  def sortStreetsByProfitability(parkingTicketsStream: InputStream): SortedMap[String, Integer] = {
    lazy val streetToFine = createStreetToFine(parkingTicketsStream)
    new TreeMap[String, Integer](new ValueComparator(streetToFine)) {
      override def get(key: Any) = {
        if (isEmpty) putAll(streetToFine.asJava)
        super.get(key)
      }
    }
  }

  private def createStreetToFine(parkingTicketsStream: InputStream) = {
    val streetToFine = mutable.Map[String, Integer]().withDefaultValue(0)
    val linesWithoutHeader = Source.fromInputStream(parkingTicketsStream).getLines.drop(1)

    for {
      fileChunk <- linesWithoutHeader.grouped(32000) //read groups of 32000 lines from the file
      parallelLineChunk <- fileChunk.grouped(8000).toList.par //each thread operates on 8000 lines
      line <- parallelLineChunk
    } {
      val columns = line.split(',') //good enough since all quoted entries contain typos
      val fine = columns(FineIndex).toInt
      val location = columns(LocationIndex)

      location match {
        case Address(_, street, _) => streetToFine.synchronized {
          streetToFine(streetWithoutSuffix(street)) += fine
        }
        case _ =>
      }
    }

    streetToFine
  }

  private def streetWithoutSuffix(street: String) = {
    val suffix = StringUtils.substringAfterLast(street, " ")
    if (StreetSuffixes.contains(suffix)) StringUtils.substringBeforeLast(street, " ") else street
  }
}