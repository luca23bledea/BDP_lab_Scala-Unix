package basics

import basics.Part2._
import org.scalatest.FunSuite

class Part2Test extends FunSuite {

	test("Sum") {
		assertResult(15) {
			sum((1 to 5).toList)
		}
	}

	test("Optional sum") {
		assertResult(16) {
			optionalSum(List(Num(2), Nothing(), Num(5), Num(3), Nothing(), Num(2), Nothing(), Nothing(), Num(4)))
		}
	}

	test("traffic light example as test") {
		assertResult(
			List(
				(false,"ok: nothing wrong here"),
				(false,"ok: nothing wrong here"),
				(false,"ok: we allow it"),
				(true,"ok: too late"),
				(true,"ok: waaay too late"),
				(false,"ERROR: (bananas,1100123)")
			)) {
			val drivers = List(
				("green", 14),
				("green", 3),
				("orange", 2),
				("orange", 6),
				("red", 1),
				("bananas", 1100123))

			drivers.map(arg => trafficLightPenalty(arg._1, arg._2))
		}
	}

	test("Q4 - twice") {
		assertResult(List("abc", "abc")) {
			twice(List("abc"))
		}
		assertResult(List(0,0,1,1,2,2,3,3)) {
			twice(List.range(0, 4))
		}
	}

	test("Q5 - drunkWords") {
		assertResult(List("uoy","yeh")) {
			drunkWords(List("hey", "you"))
		}
		assertResult(List("?gniod", "uoy", "era", "woh", ",uoy", "yeH")) {
			drunkWords(List("Hey","you,","how","are","you","doing?"))
		}
	}

	// test myForAll is skipped due to hints for 1st questions ^_^
	test("Q6 - myForAll") {
		val startsWithS = (s: String) => s.startsWith("s")
		assertResult(false) {
			myForAll(List("abc", "def"), startsWithS)
		}
		assertResult(true) {
			myForAll(List("start", "strong", "system"), startsWithS)
		}
	}

	test("Q7 - lastElem") {
		assertResult(Some("yes")) {
			lastElem(List("no", "yes", "no", "no", "yes"))
		}
		assertResult(None) {
			lastElem(List())
		}
		assertResult(Some(2)) {
			lastElem(List.range(0,3))
		}
	}

	test("Q8 - append") {
		assertResult(List(1,3,5,2,4)) {
			append(List(1,3,5), List(2,4))
		}
		assertResult(List()) {
			append(List(), List())
		}
		assertResult(List(1, 3, 5, 2, 4)) {
			append(List(1,3,5), List(2,4))
		}
	}

	// test myFilter is skipped due to hints for 1st questions ^_^
	test("Q9 - myFilter") {
		val nrs = List.range(0,11)
		assertResult(List(0, 4, 8)) {
			myFilter(nrs, (i: Int) => i % 2 == 0)
		}
	}

}