case class Vehicule(
                   MATRICULE_ID : String,
                   COLOUR : String,
                   TRANSMISSION_TYPE : String,
                   ENGINE_TYPE :String
                   ) {
  def accelerate() : Unit = {
  }


  def vitesseMax(engine_type : String, transmission : String) : Int = {
    if (engine_type == "turbo" && transmission == "auto") {
      150 // it is a return
    } else {
      100
    }

  }

}