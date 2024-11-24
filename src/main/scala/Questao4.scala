
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

  val imoveisMenosQuinhetos = dfRenomeado
    .filter(col("preco") < 500000)
    .select("id", "cidade", "bairro", "num_quartos", "preco")

  val imoveisMaisOitocentos = dfRenomeado
    .filter(col("preco") > 800000)
    .select("id", "cidade", "bairro", "num_quartos", "preco")

  // Agrupando por cidade e contando o número de empreendimentos
  val imoveisCount = imoveisValorEspecifico
    .groupBy("cidade")
    .agg(count("cidade").alias("quantidade_empreendimentos"))

  val imoveisMenosQuinhetosCount = imoveisMenosQuinhetos
    .groupBy("cidade")
    .agg(count("cidade").alias("quantidade_empreendimentos"))

  val imoveisMaisOitocentosCount = imoveisMaisOitocentos
    .groupBy("cidade")
    .agg(count("cidade").alias("quantidade_empreendimentos"))

  // Coletando os dados para plotagem
  val imoveisColeta = imoveisCount.collect().map(row =>
    (row.getAs[String]("cidade"), row.getAs[Long]("quantidade_empreendimentos"))
  )
  val imoveisMenosQuinhetosColeta = imoveisMenosQuinhetosCount.collect().map(row =>
    (row.getAs[String]("cidade"), row.getAs[Long]("quantidade_empreendimentos"))
  )
  val imoveisMaisOitocentosColeta = imoveisMaisOitocentosCount.collect().map(row =>
    (row.getAs[String]("cidade"), row.getAs[Long]("quantidade_empreendimentos"))
  )

  val cidades = imoveisColeta.map(_._1)
  val quantidades = imoveisColeta.map(_._2)

  val cidadesMenosQuinhentos = imoveisMenosQuinhetosColeta.map(_._1)
  val quantidadesMenosQuinhentos = imoveisMenosQuinhetosColeta.map(_._2)

  val cidadesMaisOitocentos = imoveisMaisOitocentosColeta.map(_._1)
  val quantidadesMaisOitocentos = imoveisMaisOitocentosColeta.map(_._2)

  // Criando o gráfico de barras
  val trace = Bar(
    x = cidades.toSeq,          // Sequência de cidades
    y = quantidades.map(_.toDouble).toSeq, // Sequência de quantidades (convertido para Double)

  ).withName("Valor entre 500 e 800 Mil")
    .withMarker(Marker().withColor(Color.RGBA(255, 165, 0, 0.6))) // Cor diferenciada para o gráfico

  val traceMenosQuinhentos = Bar(
    x = cidadesMenosQuinhentos.toSeq,          // Sequência de cidades
    y = quantidadesMenosQuinhentos.map(_.toDouble).toSeq, // Sequência de quantidades (convertido para Double)

  ).withName("Valor inferior a 500 Mil")
    .withMarker(Marker().withColor(Color.RGBA(0, 165, 255, 0.6))) // Cor diferenciada para o gráfico

  val traceMaisOitocentos = Bar(
    x = cidadesMaisOitocentos.toSeq,          // Sequência de cidades
    y = quantidadesMaisOitocentos.map(_.toDouble).toSeq, // Sequência de quantidades (convertido para Double)

  ).withName("Valor superior a 800 Mil")
    .withMarker(Marker().withColor(Color.RGBA(0, 255, 0, 0.6))) // Cor diferenciada para o gráfico


  val layout = Layout()
    .withTitle("Quantidade de Empreendimentos por Cidade de acordo com a faixa de preço")
    .withXaxis(Axis().withTitle("Cidade"))
    .withYaxis(Axis().withTitle("Quantidade de Empreendimentos"))

  // Plotando o gráfico
  plot("./graficos/questao4.html", Seq(traceMenosQuinhentos, trace, traceMaisOitocentos), layout)


  // Exibindo o resultado
  imoveisValorEspecifico.show(false)
  imoveisCount.show(false)

  // Parando o SparkSession (opcional)
  spark.stop()
}
