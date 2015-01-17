package ir.angellandros.verblab

import java.util.HashMap
	
class PairedIterable[K, V](x: Iterable[(K, V)]) {
	def reduceByKey(func: (V,V) => V) = {
		val map = new HashMap[K, V]
		x.foreach { pair =>
			val old = map.get(pair._1)
			map.put(pair._1, if (old == null) pair._2 else func(old, pair._2))
		}
		map
	}
}

implicit def iterableToPairedIterable[K, V](x: Iterable[(K, V)]) = { new PairedIterable(x) }