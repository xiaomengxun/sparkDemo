package com.erongda.bigdata.spark.mllib.classification

import com.erongda.bigdata.spark.ContantUtils
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 使用朴素贝叶斯算法预测分类：新闻标题（特征）和新闻类别（分类）
  */
object NewsCategoryClassificationTest {

  def main(args: Array[String]): Unit = {

    // a. 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName("NewsCategoryClassificationTest")
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._

    // 获取SparkContext上下文对象
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    // b. 读取分隔符为\t的csv文件，通过自定义Schema信息
    // b.1 定义Schema：ID \t TITLE \t URL \t PUBLISHER \t CATEGORY \t STORY \t HOSTNAME \t TIMESTAMP
    val schema: StructType = StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("title", StringType, nullable = true),
        StructField("url", StringType, nullable = true),
        StructField("publisher", StringType, nullable = true),
        StructField("category", StringType, nullable = true),
        StructField("story", StringType, nullable = true),
        StructField("hostname", StringType, nullable = true),
        StructField("timestamp", StringType, nullable = true)
      )
    )
    // b.2 读取数据
    val newsDF: DataFrame = spark.read
      .option("sep", "\t").schema(schema)
      .csv(ContantUtils.LOCAL_DATA_DIC + "/newsdata/newsCorpora.csv")

    // newsDF.printSchema()
    // newsDF.show(20, truncate = true)

    // TODO: 统计各个类别的数据总数，看看类别数据中是否出现数据倾斜
    // newsDF.select($"category").groupBy($"category").count().show(10, truncate = false )

    // b.3 提取特征字段数据
    val titleCategoryRDD: RDD[(Double, String)] = newsDF.select($"title", $"category").rdd
      .map(row => {
        //获取类别数据，进行转换数值类型
        val category = row.getString(1) match {
          // b = business, t = science and technology, e = entertainment, m = health
          case "b" => 0.0
          case "t" => 1.0
          case "e" => 2.0
          case "m" => 3.0
          case _ => -1
        }
        // 以二元组的形式返回
        (category, row.getString(0))
      })

    // b.4 从 新闻标题 title 提取特征值（转换为数值类型）
    /**
      * 使用词袋模型（BOW）提取文本特征数据
      *     TF * IDF
      */
    // i. 创建HashingTF, 指定单词数量为100000， 默认值2^20 = 1048576
    val hashTF = new HashingTF(100000)

    // 特征转换
    val lpsRDD: RDD[LabeledPoint] = titleCategoryRDD.map{
      case (category, newTitle) =>
        // 对新闻标题进行分词
        val words: Seq[String] = newTitle.split("\\s+")
          .map(word => word.toLowerCase.replaceAll(",", "").replaceAll("\\.", "")).toSeq
        // 将文本分割微单词Seq转换为向量（计算TF）
        val tf: Vector = hashTF.transform(words)
        // 返回
        LabeledPoint(category, tf)
    }.filter(_.label != -1)
    lpsRDD.take(5).foreach(println)

    // println("=============================================")

    // TODO: IDF 加权文本特征值
    // 获取特征
    val tfRDD: RDD[Vector] = lpsRDD.map(_.features)
    // 构建一个IDF： 通过TF值获取
    val idfModel: IDFModel = new IDF().fit(tfRDD)

    // 给TF进行加权
    val lpsTfIdfRDD: RDD[LabeledPoint] = lpsRDD.map{
      case LabeledPoint(label, features) => LabeledPoint(label, idfModel.transform(features))
    }
    // lpsTfIdfRDD.take(5).foreach(println)

    // TODO: 划分数据集为训练数据集和测试数据集
    val Array(trainingRDD, testingRDD) = lpsTfIdfRDD.randomSplit(Array(0.8, 0.2))

    // c. 使用朴素贝叶斯算法训练模型
    val nbModel: NaiveBayesModel = NaiveBayes.train(trainingRDD)

    // 使用测试数据进行预测
    val nbPredictAndActualRDD: RDD[(Double, Double)] = testingRDD.map{
      case LabeledPoint(label, features) => (nbModel.predict(features), label)
    }
    // nbPredictAndActualRDD.take(20).foreach(println)

    /**
      * TODO: 多分类模型评估指标： 精确度ACC，混淆矩阵ConfusionMatrix
      */
    // 多分类评估
    val metrics: MulticlassMetrics = new MulticlassMetrics(nbPredictAndActualRDD)

    println(s"使用朴素贝叶斯模型预测新闻分类：精确度ACC = ${metrics.accuracy}")
    // 混淆矩阵
    //println(metrics.confusionMatrix)

    /**
      * TODO：针对朴素贝叶斯算法来说，超参数：
      *   -a. lambda: Double
      *       默认值为1.0， The smoothing parameter（平滑参数，解决过拟合问题）
      *   -b. modelType: String
      *       NaiveBayes.Multinomial -> 多项式
      *       NaiveBayes.Bernoulli -> 伯努利
      */

    // d. 模型保存
    // nbModel.save(sc, ContantUtils.LOCAL_DATA_DIC + "/news-nb-model2")

    // 加载模型，进行预测
    val title = "Fed official says weak data caused by weather, should not slow taper"
    // 提取特征
    val titleWords: Seq[String] = title.split("\\s+")
        .map(word => word.toLowerCase.replaceAll(",", "").replaceAll("\\.", ""))
        .toSeq
    val features: Vector = idfModel.transform(hashTF.transform(titleWords))

    // 实际类别  b
    val predictCategory: String = NaiveBayesModel
      .load(sc, ContantUtils.LOCAL_DATA_DIC + "/news-nb-model")
      .predict(features) match {
        case 0.0 => "b"
        case 1.0 => "t"
        case 2.0 => "e"
        case 3.0 => "m"
      }
    println(s"Predict Category： $predictCategory")

    /**
      * Word2Vec 模型：将单词Word转换为向量Vector，找某个单词的相似度最近的Top个单词
      */
    val inputRDD: RDD[Seq[String]] = titleCategoryRDD.map{
      case (category, newsTitle) =>
        newsTitle.split("\\s+")
          .map(word => word.trim.toLowerCase.replace(",", "").replace("\\.", ""))
          .toSeq
    }
    // 创建Word2Vec实例对象
    val word2vec = new Word2Vec()
    // 使用数据训练模型
    val word2VecModel: Word2VecModel = word2vec.fit(inputRDD)

    // 使用模型找出某个单词前20个相近的词汇
    val synonyms: Array[(String, Double)] = word2VecModel.findSynonyms("stock".toLowerCase, 10)
    for((word, cosineSimilarity) <- synonyms){
      println(s"$word -> $cosineSimilarity")
    }

    // 线程休眠
    Thread.sleep(100000000)
    // 关闭资源
    spark.stop()
  }

}
