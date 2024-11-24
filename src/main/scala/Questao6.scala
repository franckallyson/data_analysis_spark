import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import plotly._
import element._
import layout._
import Plotly._


object Questao6 extends App {

  // Criação do SparkSession
  val spark = SparkSession.builder()
    .appName("ImoveisPrecoElevadoAnoRecente")
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
    .withColumnRenamed("Ano de construção", "ano_construcao")
    .withColumnRenamed("Preço", "preco")

  // Filtrando imóveis construídos após 2015 e com preço superior a 1.200.000
  // Ordenamos em Desc sem o filtro de preço, para verificar se realmnete não existe nenhum,
  val imoveisFiltrados = dfRenomeado
    .filter(col("preco") > 1200000 && col("ano_construcao") > 2015 )
    .orderBy(col("preco").desc)
    .select("id", "cidade", "bairro", "ano_construcao", "preco")

  // Criando uma coluna para identificar unicamente bairro e cidade
  val dfComCidadeBairro = imoveisFiltrados
    .withColumn("cidade_bairro", concat_ws(", ", col("cidade"), col("bairro")))

  val dfCountImoveisByCidadeBairro = dfComCidadeBairro
    .groupBy("cidade_bairro")
    .agg(count("cidade_bairro").alias("quantidade_empreendimentos"))


  // Coletando os dados para plotagem
  val imoveisColeta = dfCountImoveisByCidadeBairro.collect().map(row =>
    (row.getAs[String]("cidade_bairro"), row.getAs[Long]("quantidade_empreendimentos"))
  )

  val cidade_bairro = imoveisColeta.map(_._1).toSeq
  val quantidade_empreendimentos = imoveisColeta.map(_._2).toSeq

  val trace = Bar(
    x = cidade_bairro,
    y = quantidade_empreendimentos
  ).withName("Mais novos e mais caros por bairro-cidade")
    .withMarker(Marker().withColor(Color.RGBA(255, 165, 0, 0.6))) // Cor diferenciada para o gráfico

  val layout = Layout()
    .withTitle("Imóveis com preço elevado (> 1.2M) e contrução recente (> 2015)")
    .withXaxis(Axis().withTitle("Cidade - Bairro"))
    .withYaxis(Axis().withTitle("Quantidade"))
    .withShowlegend(false)

  // Plotando o gráfico
  plot("./graficos/questao6.html", Seq(trace), layout)


  // Exibindo o resultado
  imoveisFiltrados.show()

  // Parando o SparkSession (opcional)
  spark.stop()
}
