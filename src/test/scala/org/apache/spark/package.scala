package org.apache.spark

import org.scalatest._

/** abstract general testing class */
abstract class UnitTestSuite extends FunSuite with Matchers with OptionValues with Inside
  with Inspectors with BeforeAndAfterAll with BeforeAndAfter
