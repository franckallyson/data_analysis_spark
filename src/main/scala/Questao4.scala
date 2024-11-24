
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly.Bar
import plotly.Plotly.plot
import plotly.element.{Color, Marker}
import plotly.layout.{Axis, Layout}

object Questao4 extends App {

  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("ImoveisValorEspecifico")
    .master("local")
    .getOrCreate()

  // Carregando o CSV com a inferência de schema
  val dfImoveis = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("imoveis.csv")

  // Renomeando colunas para facilitar o acesso
  val dfRenomeado = dfImoveis
    .withColumnRenamed("ID", "id")
    .withColumnRenamed("Cidade", "cidade")
    .withColumnRenamed("Bairro", "bairro")
    .withColumnRenamed("Número de quartos", "num_quartos")
    .withColumnRenamed("Preço", "preco")

  // Selecionando imóveis com preço entre 500.000 e 800.000
  val imoveisValorEspecifico = dfRenomeado
    .filter(col("preco") >= 500000 && col("preco") <= 800000)
    .select("id", "cidade", "bairro", "num_quartos", "preco")

  // Agrupando por cidade e contando o número de empreendimentos
  val imoveisCount = imoveisValorEspecifico
    .groupBy("cidade")
    .agg(count("cidade").alias("quantidade_empreendimentos"))

  // Coletando os dados para plotagem
  val imoveisColeta = imoveisCount.collect().map(row =>
    (row.getAs[String]("cidade"), row.getAs[Long]("quantidade_empreendimentos"))
  )

  val cidades = imoveisColeta.map(_._1)
  val quantidades = imoveisColeta.map(_._2)

  // Criando o gráfico de barras
  val trace = Bar(
    x = cidades.toSeq,          // Sequência de cidades
    y = quantidades.map(_.toDouble).toSeq, // Sequência de quantidades (convertido para Double)

  ).withName("Quantidade de Empreendimentos")
    .withMarker(Marker().withColor(Color.RGBA(255, 165, 0, 0.6))) // Cor diferenciada para o gráfico

  val layout = Layout()
    .withTitle("Quantidade de Empreendimentos por Cidade (500.000 < Preço > 800.000)")
    .withXaxis(Axis().withTitle("Cidade"))
    .withYaxis(Axis().withTitle("Quantidade de Empreendimentos"))
    .withShowlegend(false)

  // Plotando o gráfico
  plot("./graficos/questao4.html", Seq(trace), layout)



  // Exibindo o resultado
  imoveisValorEspecifico.show(false)
  imoveisCount.show(false)

  // Parando o SparkSession (opcional)
  spark.stop()
}
