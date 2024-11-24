import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly._, element._, layout._, Plotly._

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

  val proximosCentroMedia = proximosCentro
    .groupBy("cidade")
    .agg(avg("preco").alias("preco_medio"))

  // Coletando os dados para plotagem
  val proximosCentroColeta = proximosCentroMedia.collect().map(row =>
    (row.getAs[String]("cidade"), row.getAs[Double]("preco_medio"))
  )

  val cidades = proximosCentroColeta.map(_._1)
  val medias = proximosCentroColeta.map(_._2)

  // Criando o gráfico de barras
  val trace = Bar(
    x = cidades.toSeq,          // Sequência de cidades
    y = medias.map(_.toDouble).toSeq, // Sequência de quantidades (convertido para Double)

  ).withName("Quantidade de Empreendimentos")
    .withMarker(Marker().withColor(Color.RGBA(255, 165, 0, 0.6))) // Cor diferenciada para o gráfico

  val layout = Layout()
    .withTitle("Preço Médio dos Empreendimentos próximos ao Centro")
    .withXaxis(Axis().withTitle("Cidade"))
    .withYaxis(Axis().withTitle("Preço Médio"))
    .withShowlegend(false)

  // Plotando o gráfico
  plot("./graficos/questao3.html", Seq(trace), layout)

  // Exibindo o resultado
  proximosCentro.show(false)
  proximosCentroMedia.show(false)

  // Parando o SparkSession (opcional)
  spark.stop()
}
