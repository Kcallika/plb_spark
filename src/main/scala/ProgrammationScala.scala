// import scala.collection.mutable
import scala.collection.immutable

object ProgrammationScala {

   // public variable of base type (not class type)
  // string, bool, unit (no result returned), int... and any
  // declaration and init is at the same type (here)

  val session_kafka : Int = 15 // immutable public var
  var session_kafka_mu : Int = 15 // mutable var
  val wo_type_str = "my text" // scala detects type automatically -- not recommended


  val renault : Vehicule = new Vehicule (MATRICULE_ID = "hkk", COLOUR = "red", TRANSMISSION_TYPE = "auto", ENGINE_TYPE = "turbo" )
  // renault.COLOUR = "green" // error

  def main (args: Array[String]) : Unit = {
    println("Hello World, Scala")
    fonction2()

    val colour : String = renault.COLOUR
    renault.accelerate()
    println("My auto colour is: " + colour)
    renault.vitesseMax(engine_type = "turbo", transmission = "auto")

    testHello("Youpi!")
    val res = testABC(10, 150)
    println(res)


    structureData()
  }

  def fonction2() : Unit = {
    // session_kafka = 18 // error -- immutable
    session_kafka_mu = 18
    println("Variable value: " +  session_kafka)
    println("Variable value: " +  session_kafka_mu)
  }

  // methode --- execute an action

  def testHello(text: String) : Unit = {
    val a = 10
    val b = 15
    val c = a + b
    println("Here is your message: " +  text)
    return c  // wil be ignored
  }

  def testHelloF(text: String) : Int = {
    val a = 10
    val b = 15
    val c = a + b
    println("Here is your message: " +  text)
    return c  // wil not be ignored
  }

  def testABC(a: Int, b : Int) : Int = {
    val c = a + b
    return c
  }


  def conditionalStructure() : Unit = {
    var i = 0
    while (i < 7) {
      println(i)
      i = i + 1
    }

    var j = 0
    for (j <- 0 to 10) {
      println(j)
    }

    if (i  > j){
      println("i is greater")
    } else {
      println("i is smaller")
    }
  }

  // collections : list, tuple, set, map
  // list: immutable, organised (ordered --- you can access with idx) and data is of same type

  def structureData() : Unit = {
    // lists

    // lists with Int, Strig and Range
    val list1 : List[Int] = List(1,8, 5, 6, 9, 59)
    val list_names : List[String] = List("Oxana", "Alex", "Valentin")
    val nums = List.range(0, 15)

    // manipulation
    for (name  <- list_names) {
      println(name)
    }

    println("====================")

    // anonymous function as python lambda
    val list_maj : List[String] = list_names.map(e => e.toUpperCase)
    list_maj.foreach(e => println(e) )

    println("====================")
    // all names by V
    val list_v = list_names.map(i => i.startsWith("V")) // applies on each elem we could use filter instead
    list_v.foreach(i => println(i))

    // list * 2
    val list_mult = list1.map(i => i*2 )
    list_mult.foreach(i => println(i))

    // list val greater 5
    val list_greater :  List[Int] = list1.filter(_ > 5) // i => i > 5
    list_greater.foreach(i => println(i))
  }


  // tuples
  // values are from different types
  val my_tuple: (String, Int, Boolean) = ("oxa", 49, true)
  val other_tuple = (45, "lion", "unicorn", false)
  val all_tuple = ("kitten", 24, Vehicule(MATRICULE_ID = "hkk", COLOUR = "red", TRANSMISSION_TYPE = "auto", ENGINE_TYPE = "turbo"))

  println(other_tuple._3.toUpperCase)

  // map of Hash Tab
  val cities : Map[String, String] = Map(
    "PS" -> "Paris",
    "LS" -> "Lyon",
    "MA" -> "Marseille"
  )

  cities.foreach(k  => println("key" + k._1 + " value" + k._2))

  // tables : mutable lists
  val tab : Array[String] = Array("Oxana", "Alex", "Valentin", "Unicorn")
  tab(0) = "Julien"

  var i = 0
  for (i <- 0 to 3 ) {
    println(tab(i))
  }
}
