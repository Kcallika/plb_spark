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
}
