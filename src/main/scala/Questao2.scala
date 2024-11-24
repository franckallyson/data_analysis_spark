import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly._
import element._
import layout._
import Plotly._
import plotly.element.BarTextPosition.Outside
import plotly.element.Error.Data

object Questao2 extends App {

  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("PrecoMedioImoveis")
    .master("local")
    .getOrCreate()

  // Carregando o CSV com a inferência de schema
  val dfImoveis = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("imoveis.csv")

  // Renomeando a coluna de "Preço" para facilitar o acesso
  val dfRenomeado = dfImoveis
    .withColumnRenamed("Preço", "preco")

  // Calculando o preço médio
  val precoMedio = dfRenomeado
    .agg(
      avg("preco").alias("preco_medio"),
      bround(stddev("preco"), 2).alias("desvio_padrao"),
    )

  val precoMedioCollected = precoMedio.collect()

  val precoSeq = precoMedioCollected.map(_.getDouble(0)).toSeq
  val desvSeq = precoMedioCollected.map(_.getDouble(1)).toSeq

  val traceMedia = Bar(
    x = Seq("Imóveis"),
    y = precoSeq,

  ).withName("Preço Médio")
    .withError_y(
      Data(array=desvSeq)
        .withVisible(true)

    )

  val layout = Layout()
    .withTitle("Preço Médio dos imóveis com Desvio Padrão")
    .withXaxis(Axis().withTitle("Imóveis"))
    .withYaxis(Axis().withTitle("Preço (R$)"))

  // Gerando o gráfico
  plot(
    "./graficos/questao2.html",
    Seq(traceMedia),
    layout,
    useCdn = true,
    openInBrowser = true
  )

  // Exibindo apenas o valor da média
  precoMedio.select("preco_medio").show()

  // Parando o SparkSession (opcional)
  spark.stop()
}
