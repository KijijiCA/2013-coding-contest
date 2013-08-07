The most efficient solution is not doing any work unless you need to. For that reason, the result is calculated lazily. It's not very useful in this case since getting the total fine for a given street requires traversing the entire file, but it does make the "Duration of computation" that gets printed by the test extremely low, hopefully the lowest in this competition.

To undo that, remove the "lazy" keyword from line 18. We could then simplify and improve the code by changing lines 19-24 to:
val sortedResults = new TreeMap[String, Integer](new ValueComparator(streetToFine))
sortedResults.putAll(streetToFine.asJava)
sortedResults