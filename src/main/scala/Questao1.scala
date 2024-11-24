import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly._, element._, layout._, Plotly._

object Questao1 extends App {

  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("Imoveis")
    .master("local")
    .getOrCreate()

  // Carregando o CSV com a inferência de schema
  val dfImoveis = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("imoveis.csv")

  dfImoveis.printSchema()

  // Renomeando as colunas para facilitar o acesso
  val dfRenomeado = dfImoveis
    .withColumnRenamed("ID", "id")
    .withColumnRenamed("Área (m²)", "area")
    .withColumnRenamed("Preço", "preco")
    .withColumnRenamed("Cidade", "cidade")
    .withColumnRenamed("Bairro", "bairro")

  // Selecionando os imóveis com área > 100 m² e preço > 1.000.000
  val resultado = dfRenomeado
    .filter(col("area") > 100 && col("preco") > 1000000)
    .select("id", "cidade", "bairro", "preco")

  // Agrupando por cidade e contando o número de empreendimentos
  val resultadoAgrupado = resultado
    .groupBy("cidade")
    .agg(count("cidade").alias("quantidade_empreendimentos"))

  // Coletando os dados para plotagem
  val resultadoColetado = resultadoAgrupado.collect().map(row =>
    (row.getAs[String]("cidade"), row.getAs[Long]("quantidade_empreendimentos"))
  )

  val cidades = resultadoColetado.map(_._1)
  val quantidades = resultadoColetado.map(_._2)

  // Criando o gráfico de barras
  val trace = Bar(
    x = cidades.toSeq,          // Sequência de cidades
    y = quantidades.map(_.toDouble).toSeq, // Sequência de quantidades (convertido para Double)

  ).withName("Quantidade de Empreendimentos")
    .withMarker(Marker().withColor(Color.RGBA(255, 165, 0, 0.6))) // Cor diferenciada para o gráfico

  val layout = Layout()
    .withTitle("Quantidade de Empreendimentos por Cidade (Área > 100 m² e Preço > 1.000.000)")
    .withXaxis(Axis().withTitle("Cidade"))
    .withYaxis(Axis().withTitle("Quantidade de Empreendimentos"))
    .withShowlegend(false)

  // Plotando o gráfico
  plot("./graficos/questao1.html", Seq(trace), layout)

  resultado.show(false)
  resultadoAgrupado.show(false)

  spark.stop()
}
