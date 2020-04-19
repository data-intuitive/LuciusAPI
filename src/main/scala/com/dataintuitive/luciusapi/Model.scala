package com.dataintuitive.luciusapi

object Model {

  case class FlatDbRow(id: String, protocol: String, concentration:String, compoundType:String, compoundId: String, informative: Boolean)

}
