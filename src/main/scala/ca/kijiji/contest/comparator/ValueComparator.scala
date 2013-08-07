package ca.kijiji.contest.comparator

import java.util.Comparator
import scala.collection.mutable

class ValueComparator[K <: Comparable[K], V <: Comparable[V]](map: => mutable.Map[K, V]) extends Comparator[K] {
  override def compare(key1: K, key2: K): Int = {
    val value1 = map(key1)
    val value2 = map(key2)
    if (value1 == value2) key2.compareTo(key1) //if values are equal we need to compare the key, otherwise entries are lost
    else value2.compareTo(value1)
  }
}