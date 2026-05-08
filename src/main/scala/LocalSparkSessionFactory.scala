
import java.io.IOException
import java.nio.file.Files
import java.util.LinkedHashMap

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession

final class LocalSparkSessionFactory {

  def create(config: LocalSparkSessionFactoryConfig): SparkSession = {
    require(config != null, "config")
    val warehouseDir = Option(config.getWarehouseDir)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(createWarehouseDir())

    var builder = SparkSession.builder()
      .appName(config.getAppName)
      .master(config.getMaster)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.sql.legacy.createHiveTableByDefault", "false")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.default.parallelism", "1")
      .config(
        "spark.sql.catalog.dataplat_hadoop_catalog",
        "org.apache.spark.sql.flow.DataplatHadoopCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)

    config.getExtraConfigs.asScala.foreach { case (key, value) =>
      builder = builder.config(key, value)
    }

    val sparkSession = builder.getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

  private def createWarehouseDir(): String = {
    try {
      Files.createTempDirectory("spark-local-warehouse-").toAbsolutePath.toString
    } catch {
      case err: IOException =>
        throw new IllegalStateException("Failed to create local Spark warehouse directory", err)
    }
  }
}

final class LocalSparkSessionFactoryConfig {
  private var appName: String = "jdbc-spark-catalog-bootstrap"
  private var master: String = "local[1]"
  private var warehouseDir: String = _
  private val extraConfigs = new LinkedHashMap[String, String]()

  def setAppName(appName: String): LocalSparkSessionFactoryConfig = {
    this.appName = java.util.Objects.requireNonNull(appName, "appName")
    this
  }

  def setMaster(master: String): LocalSparkSessionFactoryConfig = {
    this.master = java.util.Objects.requireNonNull(master, "master")
    this
  }

  def setWarehouseDir(warehouseDir: String): LocalSparkSessionFactoryConfig = {
    this.warehouseDir = warehouseDir
    this
  }

  def putConfig(key: String, value: String): LocalSparkSessionFactoryConfig = {
    extraConfigs.put(key, value)
    this
  }

  def getAppName: String = appName

  def getMaster: String = master

  def getWarehouseDir: String = warehouseDir

  def getExtraConfigs: LinkedHashMap[String, String] = extraConfigs
}
