import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly._
import element._
import layout._
import Plotly._
import plotly.element.BarTextPosition.Outside
import plotly.element.Error.Data

object Questao5 extends App {

  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("MediaNumQuartos")
    .master("local")
    .getOrCreate()

  // Carregando o CSV com a inferência de schema
  val dfImoveis = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("imoveis.csv")

  // Renomeando a coluna "Número de quartos" para facilitar o acesso
  val dfRenomeado = dfImoveis
    .withColumnRenamed("Número de quartos", "num_quartos")

  // Calculando a média do número de quartos
  val mediaNumQuartos = dfRenomeado
    .agg(
      avg("num_quartos").alias("media_num_quartos"),
      bround(stddev("num_quartos"), 2).alias("desvio_padrao"),
    )

  val mediaNumQuartosCollected = mediaNumQuartos.collect()

  val mediaQuartosSeq = mediaNumQuartosCollected.map(_.getDouble(0)).toSeq
  val desvSeq = mediaNumQuartosCollected.map(_.getDouble(1)).toSeq

  val traceMedia = Bar(
    x = Seq("Número de Quartos"),
    y = mediaQuartosSeq,

  )
    .withError_y(
      Data(array=desvSeq)
        .withVisible(true)

    )

  val layout = Layout()
    .withTitle("Quantidade Média de quartos com desvio padrão")
    .withXaxis(Axis().withTitle("Quartos"))
    .withYaxis(Axis().withTitle("Quantidade"))

  // Gerando o gráfico
  plot(
    "./graficos/questao5.html",
    Seq(traceMedia),
    layout,
    useCdn = true,
    openInBrowser = true
  )

  // Exibindo apenas o valor da média
  mediaNumQuartos.select("media_num_quartos").show()

  // Parando o SparkSession (opcional)
  spark.stop()
}
