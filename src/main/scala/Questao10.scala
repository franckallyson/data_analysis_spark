import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly._
import plotly.element.Error.Data

object Questao10 extends App {

  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("GraficoDeLinhasPrecos")
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
    .withColumnRenamed("Preço", "preco")

  // Criando faixas de número de quartos
  val dfComFaixasDeQuartos = dfRenomeado
    .withColumn("faixa_quartos",
      when(col("num_quartos") <= 2, "1-2")
        .when(col("num_quartos") <= 4, "3-4")
        .otherwise("4+")
    )

  // Calculando o preço médio, preço mais barato e preço mais caro por faixa de número de quartos
  val faixaPrecoPorQuartos = dfComFaixasDeQuartos
    .orderBy("faixa_quartos")
    .groupBy("faixa_quartos")
    .agg(
      bround(avg("preco"), 2).alias("preco_medio"),
      bround(min("preco").cast("double"), 2).alias("preco_minimo"),
      bround(max("preco").cast("double"), 2).alias("preco_maximo"),
      bround(stddev("preco"), 2).alias("desvio_padrao"),
      //bround(avg(abs(col("preco") - avg("preco").over())), 2).alias("desvio_medio")
    )

  // Coletando os resultados para uso no gráfico
  val faixaQuartos = faixaPrecoPorQuartos.collect()

  val faixas = faixaQuartos.map(_.getString(0)).toSeq // Faixas de quartos
  val precosMedios = faixaQuartos.map(_.getDouble(1)).toSeq // Preços médios
  val precosMinimos = faixaQuartos.map(_.getDouble(2)).toSeq // Preços mínimos
  val precosMaximos = faixaQuartos.map(_.getDouble(3)).toSeq // Preços máximos
  val desviosPadroes = faixaQuartos.map(_.getDouble(4)).toSeq // Desvio padrão
  //val desviosMedios = faixaQuartos.map(_.getDouble(5)).toSeq // Desvio médio

  // Criando o gráfico de linhas
  val traceMedio = Bar(
    x = faixas,
    y = precosMedios
  ).withName("Preço Médio")
    .withError_y(
      Data(array=desviosPadroes)
      .withVisible(true)          // Mostrar barras de erro
    )
    .withMarker(Marker().withColor(Color.RGBA(0, 0, 255, 0.6)))


  val traceMinimo = Bar(
    x = faixas,
    y = precosMinimos,

  ).withName("Preço Mínimo")
    .withMarker(Marker().withColor(Color.RGBA(255, 100, 0, 0.6)))

  val traceMaximo = Bar(
    x = faixas,
    y = precosMaximos,

  ).withName("Preço Máximo")
    .withMarker(Marker().withColor(Color.RGBA(0, 255, 0, 0.6)))
  val layout = Layout()
    .withTitle("Preços por Faixa de Quartos")
    .withXaxis(Axis().withTitle("Faixa de Quartos"))
    .withYaxis(Axis().withTitle("Preço (R$)"))

  // Gerando o gráfico
  plot(
    "./graficos/questao10.html",
    Seq(traceMinimo, traceMedio, traceMaximo),
    layout,
    useCdn = true,
    openInBrowser = true
  )

  faixaPrecoPorQuartos.show(false)
  // Parando o SparkSession
  spark.stop()
}
