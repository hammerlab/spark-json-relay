package org.apache.spark

import org.scalatest._

import org.json4s.jackson.{JsonMethods => Json}

/** abstract general testing class */
abstract class UnitTestSuite extends FunSuite with Matchers with OptionValues with Inside
  with Inspectors with BeforeAndAfterAll with BeforeAndAfter {

  protected def checkJson(json: String, expectedJson: String): Unit = {
    Json.parse(json) should be (Json.parse(expectedJson))
  }
}
