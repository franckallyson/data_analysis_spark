import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly._
import element._
import layout._
import Plotly._
import plotly.element.BarTextPosition.Outside
import plotly.element.Error.Data

object Questao8 extends App {

  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("MediaAreaImoveis")
    .master("local")
    .getOrCreate()

  // Carregando o CSV com a inferência de schema
  val dfImoveis = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("imoveis.csv")

  // Renomeando a coluna "Área (m²)" para facilitar o acesso
  val dfRenomeado = dfImoveis
    .withColumnRenamed("Área (m²)", "area_m2")

  // Calculando a média da área
  val mediaArea = dfRenomeado
    .agg(
      avg("area_m2").alias("media_area"),
      bround(stddev("area_m2"), 2).alias("desvio_padrao"),
    )

  val areaMediaCollected = mediaArea.collect()

  val areaSeq = areaMediaCollected.map(_.getDouble(0)).toSeq
  val desvSeq = areaMediaCollected.map(_.getDouble(1)).toSeq

  val traceMedia = Bar(
    x = Seq("Imóveis"),
    y = areaSeq,

  ).withName("Preço Médio")
    .withError_y(
      Data(array=desvSeq)
        .withVisible(true)

    )

  val layout = Layout()
    .withTitle("Área Média dos imóveis com Desvio Padrão")
    .withXaxis(Axis().withTitle("Imóveis"))
    .withYaxis(Axis().withTitle("Área"))

  // Gerando o gráfico
  plot(
    "./graficos/questao8.html",
    Seq(traceMedia),
    layout,
    useCdn = true,
    openInBrowser = true
  )

  // Exibindo apenas o valor da média
  mediaArea.show(false)

  // Parando o SparkSession (opcional)
  spark.stop()
}
