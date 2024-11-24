import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly._
import element._
import layout._
import Plotly._
import plotly.element.Orientation.Horizontal

object Questao9 extends App {

  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("PrecoMedioPorBairro")
    .master("local")
    .getOrCreate()

  // Carregando o CSV com a inferência de schema
  val dfImoveis = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("imoveis.csv")

  // Renomeando as colunas para facilitar o acesso
  val dfRenomeado = dfImoveis
    .withColumnRenamed("ID", "id")
    .withColumnRenamed("Cidade", "cidade")
    .withColumnRenamed("Bairro", "bairro")
    .withColumnRenamed("Preço", "preco")

  // Criando uma coluna para identificar unicamente bairro e cidade
  val dfComCidadeBairro = dfRenomeado
    .withColumn("cidade_bairro", concat_ws(", ", col("cidade"), col("bairro")))

  // Calculando o preço médio por bairro
  val precoMedioPorBairro = dfComCidadeBairro
    .groupBy("cidade_bairro")
    .agg(bround(avg("preco"), 2).alias("preco_medio"))

  // Selecionando os 5 bairros mais caros e mais baratos
  val top5BairrosCaros = precoMedioPorBairro
    .orderBy(desc("preco_medio"))
    .limit(5)
    .collect()
    .map(row => (row.getString(0), row.getDouble(1))) // (cidade_bairro, preco_medio)
    .sortBy(_._2) // Ordena novamente em ordem decrescente

  val bairrosCaros = top5BairrosCaros.map(_._1)
  val precosCaros = top5BairrosCaros.map(_._2)


  val top5BairrosBaratos = precoMedioPorBairro
    .orderBy("preco_medio")
    .limit(5)
    .collect()
    .map(row => (row.getString(0), row.getDouble(1))) // (cidade_bairro, preco_medio)
    .sortBy(_._2)

  val bairrosBaratos = top5BairrosBaratos.map(_._1)
  val precosBaratos = top5BairrosBaratos.map(_._2)


  // Criando os gráficos de barras horizontais
  val traceCaros = Bar(
    x = precosCaros.toSeq,         // Preços médios
    y = bairrosCaros.toSeq,        // Bairros
  ).withName("Mais Caros")
    .withMarker(Marker().withColor(Color.RGBA(255, 99, 71, 0.6))) // Cor personalizada
    .withOrientation(Horizontal)

  val traceBaratos = Bar(
    x = precosBaratos.toSeq,       // Preços médios
    y = bairrosBaratos.toSeq,      // Bairros

  ).withName("Mais Baratos")
    .withMarker(Marker().withColor(Color.RGBA(70, 130, 180, 0.6))) // Cor personalizada
    .withOrientation(Horizontal)

  // Layout dos gráficos
  val layout = Layout()
    .withTitle("Ranking de Preço Médio dos Bairros (Top 5 Caros/Baratos)")
    .withXaxis(Axis().withTitle("Preço Médio (R$)"))
    .withYaxis(Axis().withTitle("Bairros").withAutomargin(true))
    .withBarmode(BarMode.Group)

  // Plotando os gráficos
  plot(
    "./graficos/questao9.html",
    Seq(traceBaratos, traceCaros),
    layout,
    config = Config().withShowEditInChartStudio(false),
    useCdn = true,
    openInBrowser = true
  )


  println("\nPreço médio por bairros (Ordem Decrescente):")
  precoMedioPorBairro
    .orderBy(desc("preco_medio"))
    .show(numRows = 5)

  println("Preço médio por bairros (Ordem Crescente):")
  precoMedioPorBairro
    .orderBy("preco_medio")
    .show(numRows = 5)

  // Parando o SparkSession (opcional)
  spark.stop()
}
