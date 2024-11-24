import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly._, element._, layout._, Plotly._
import plotly.element.Error.Data

object Questao3 extends App {

  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("ImoveisProximosCentro")
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
    .withColumnRenamed("Área (m²)", "area")
    .withColumnRenamed("Preço", "preco")
    .withColumnRenamed("Distância do centro", "distancia_centro")

  // Selecionando imóveis com distância do centro inferior a 5 km
  val proximosCentro = dfRenomeado
    .filter(col("distancia_centro") < 5)
    .select("id", "cidade", "bairro", "area", "preco")

  val distantesCentro = dfRenomeado
    .filter(col("distancia_centro") > 5)
    .select("id", "cidade", "bairro", "area", "preco")

  val proximosCentroMedia = proximosCentro
    .groupBy("cidade")
    .agg(avg("preco").alias("preco_medio"),
      stddev("preco").alias("desvio_padrao"))

  val distantesCentroMedia = distantesCentro
    .groupBy("cidade")
    .agg(avg("preco").alias("preco_medio"),
      stddev("preco").alias("desvio_padrao"))

  // Coletando os dados para plotagem
  val proximosCentroColeta = proximosCentroMedia.collect().map(row =>
    (row.getAs[String]("cidade"), row.getAs[Double]("preco_medio"), row.getAs[Double]("desvio_padrao"))
  )

  val distantesCentroColeta = distantesCentroMedia.collect().map(row =>
    (row.getAs[String]("cidade"), row.getAs[Double]("preco_medio"), row.getAs[Double]("desvio_padrao"))
  )

  val cidades = proximosCentroColeta.map(_._1)
  val medias = proximosCentroColeta.map(_._2)
  val stdDev = proximosCentroColeta.map(_._3)

  val cidadesDistantes = distantesCentroColeta.map(_._1)
  val mediasDistantes = distantesCentroColeta.map(_._2)
  val stdDevDistantes = distantesCentroColeta.map(_._3)

  // Criando o gráfico de barras
  val trace = Bar(
    x = cidades.toSeq,          // Sequência de cidades
    y = medias.map(_.toDouble).toSeq, // Sequência de quantidades (convertido para Double
  ).withName("Próximos do centro")
    .withError_y(
      Data(array=stdDev.toSeq)
        .withVisible(true)
    )
    .withMarker(Marker().withColor(Color.RGBA(255, 165, 0, 0.6))) // Cor diferenciada para o gráfico

  val trace1 = Bar(
    x = cidadesDistantes.toSeq,          // Sequência de cidades
    y = mediasDistantes.map(_.toDouble).toSeq, // Sequência de quantidades (convertido para Double
  ).withName("Distantes do centro")
    .withError_y(
      Data(array=stdDevDistantes.toSeq)
        .withVisible(true)
    )
    .withMarker(Marker().withColor(Color.RGBA(0, 165, 255, 0.6))) // Cor diferenciada para o gráfico

  val layout = Layout()
    .withTitle("Preço Médio dos Empreendimentos próximos/distantes (+- 5km) do Centro ")
    .withXaxis(Axis().withTitle("Cidade"))
    .withYaxis(Axis().withTitle("Preço Médio"))

  // Plotando o gráfico
  plot("./graficos/questao3.html", Seq(trace, trace1), layout)

  // Exibindo o resultado
  proximosCentro.show(false)
  proximosCentroMedia.show(false)

  // Parando o SparkSession (opcional)
  spark.stop()
}
