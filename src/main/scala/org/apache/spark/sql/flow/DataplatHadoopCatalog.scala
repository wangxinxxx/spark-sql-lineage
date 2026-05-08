package org.apache.spark.sql.flow

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{
  NamespaceAlreadyExistsException,
  NoSuchNamespaceException,
  NoSuchTableException,
  TableAlreadyExistsException
}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  NamespaceChange,
  SupportsRead,
  SupportsNamespaces,
  Table,
  TableCapability,
  TableCatalog,
  TableChange,
  V1Table
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReader,
  PartitionReaderFactory,
  Scan,
  ScanBuilder
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataplatHadoopCatalog extends TableCatalog with SupportsNamespaces {
  private var catalogName: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }

  override def name(): String = catalogName

  override def defaultNamespace(): Array[String] = {
    Array(session.sessionState.catalog.getCurrentDatabase)
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val database = singlePartNamespace(namespace)
    session.sessionState.catalog.listTables(database).map { table =>
      Identifier.of(Array(table.database.getOrElse(database)), table.table)
    }.toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val table = session.sessionState.catalog.getTableMetadata(toTableIdentifier(ident))
    if (table.tableType == CatalogTableType.VIEW) {
      V1Table(table)
    } else {
      LineageReadableTable(table.qualifiedName, table.schema)
    }
  }

  override def invalidateTable(ident: Identifier): Unit = {}

  override def tableExists(ident: Identifier): Boolean = {
    session.sessionState.catalog.tableExists(toTableIdentifier(ident))
  }

  override def listNamespaces(): Array[Array[String]] = {
    session.sessionState.catalog.listDatabases().map(db => Array(db)).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    if (namespace.isEmpty) {
      listNamespaces()
    } else if (namespaceExists(namespace)) {
      Array.empty
    } else {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    namespace.length == 1 && session.sessionState.catalog.databaseExists(namespace.head)
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace)
    }
    Map.empty[String, String].asJava
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    throw new TableAlreadyExistsException(ident)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException(
      s"Catalog '$catalogName' is read-only in local lineage analysis")
  }

  override def dropTable(ident: Identifier): Boolean = {
    throw new UnsupportedOperationException(
      s"Catalog '$catalogName' is read-only in local lineage analysis")
  }

  override def purgeTable(ident: Identifier): Boolean = {
    dropTable(ident)
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException(
      s"Catalog '$catalogName' is read-only in local lineage analysis")
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = {
    throw new NamespaceAlreadyExistsException(namespace)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new UnsupportedOperationException(
      s"Catalog '$catalogName' is read-only in local lineage analysis")
  }

  override def dropNamespace(namespace: Array[String]): Boolean = {
    throw new UnsupportedOperationException(
      s"Catalog '$catalogName' is read-only in local lineage analysis")
  }

  private def session: SparkSession = {
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).getOrElse {
      throw new IllegalStateException(
        s"Catalog '$catalogName' requires an active SparkSession")
    }
  }

  private def toTableIdentifier(ident: Identifier): TableIdentifier = {
    val namespace = ident.namespace()
    namespace.length match {
      case 0 =>
        TableIdentifier(ident.name(), Some(session.sessionState.catalog.getCurrentDatabase))
      case 1 =>
        TableIdentifier(ident.name(), Some(namespace.head))
      case _ =>
        throw new NoSuchTableException(ident)
    }
  }

  private def singlePartNamespace(namespace: Array[String]): String = {
    namespace.length match {
      case 0 => session.sessionState.catalog.getCurrentDatabase
      case 1 =>
        if (namespaceExists(namespace)) namespace.head else throw new NoSuchNamespaceException(namespace)
      case _ => throw new NoSuchNamespaceException(namespace)
    }
  }
}

private final case class LineageReadableTable(tableName: String, tableSchema: StructType)
  extends Table with SupportsRead {

  override def name(): String = tableName

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new LineageScanBuilder(tableSchema)
  }
}

private final class LineageScanBuilder(tableSchema: StructType) extends ScanBuilder {
  override def build(): Scan = new LineageScan(tableSchema)
}

private final class LineageScan(tableSchema: StructType) extends Scan {
  override def readSchema(): StructType = tableSchema

  override def toBatch(): Batch = LineageBatch
}

private object LineageBatch extends Batch {
  override def planInputPartitions(): Array[InputPartition] = Array.empty

  override def createReaderFactory(): PartitionReaderFactory = LineagePartitionReaderFactory
}

private object LineagePartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new UnsupportedOperationException("Lineage-only table does not support execution")
  }
}
