package br.com.smtx

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class nasa_challenge_test extends FunSuite {
  test("Teste de função para substituir [ ") {
    val resultado = nasa_challenge.replaceBracket("[2019/10/01")
    assert(resultado === "2019/10/01")
  }
  test("Teste de função para formatar data ") {
    val resultado = nasa_challenge.formatData("1 2 3 4 5 6 7 8 9 10 11 12")
    assert(resultado === Seq("1", "2", "3", "4", "5", "6 7 8 9", "11", "12"))
  }
}


