
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly.Bar
import plotly.Plotly.plot
import plotly.element.{Color, Marker}
import plotly.layout.{Axis, Layout}
import plotly.element.Error.Data

object Questao7 extends App {

  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("ImoveisComTresBanheiros")
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
    .withColumnRenamed("Número de quartos", "num_quartos")
    .withColumnRenamed("Número de banheiros", "num_banheiros")
    .withColumnRenamed("Preço", "preco")

  // Selecionando imóveis com pelo menos 3 banheiros
  val imoveisComTresBanheiros = dfRenomeado
    .filter(col("num_banheiros") >= 3)
    .orderBy(col("num_quartos").desc)
    .select("id", "cidade", "bairro", "num_quartos", "num_banheiros", "preco")

  val imoveisPrecoMedio = imoveisComTresBanheiros
    .groupBy("cidade")
    .agg(
      avg(col("preco")).alias("preco_medio"),
      stddev(col("preco")).alias("desvio_padrao")
    )

  // Coletando os dados para plotagem
  val imoveisColeta = imoveisPrecoMedio.collect().map(row =>
    (row.getAs[String]("cidade"), row.getAs[Double]("preco_medio"), row.getAs[Double]("desvio_padrao"))
  )

  val cidades = imoveisColeta.map(_._1)
  val precoMedio = imoveisColeta.map(_._2)
  val desvioPadrao = imoveisColeta.map(_._3)

  // Criando o gráfico de barras
  val trace = Bar(
    x = cidades.toSeq,          // Sequência de cidades
    y = precoMedio.toSeq, // Sequência de quantidades (convertido para Double)

  ).withName("Quantidade de Empreendimentos")
    .withError_y(
      Data(array=desvioPadrao)
        .withVisible(true)

    )
    .withMarker(Marker().withColor(Color.RGBA(255, 165, 0, 0.6))) // Cor diferenciada para o gráfico

  val layout = Layout()
    .withTitle("Preço médio dos empreendimento com mais de 3 banheiros por cidade")
    .withXaxis(Axis().withTitle("Cidade"))
    .withYaxis(Axis().withTitle("Preço Médio"))
    .withShowlegend(false)

  // Plotando o gráfico
  plot("./graficos/questao7.html", Seq(trace), layout)


  // Exibindo o resultado
  imoveisComTresBanheiros.show(false)
  imoveisPrecoMedio.show(false)

  // Parando o SparkSession (opcional)
  spark.stop()
}
