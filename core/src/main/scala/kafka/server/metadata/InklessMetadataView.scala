// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/

package kafka.server.metadata

import io.aiven.inkless.control_plane.MetadataView
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.storage.internals.log.LogConfig

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Supplier
import java.util.Properties
import scala.jdk.CollectionConverters._

/**
 * Supplies Kafka metadata to the built-in Inkless engine through its own [[MetadataView]] contract.
 * Kafka routing depends on [[DisklessTopicView]]; only the built-in provider receives this adapter.
 */
class InklessMetadataView(cache: KRaftMetadataCache, val defaultConfig: Supplier[util.Map[String, Object]])
  extends KafkaDisklessTopicView(cache) with MetadataView {

  /**
   * Cached LogConfig per topic, analogous to how classic Kafka stores a LogConfig in each
   * LocalLog instance (via LogManager -> UnifiedLog -> LocalLog.config).
   *
   * Without this cache, every produce request would reconstruct a LogConfig via
   * LogConfig.fromProps — an expensive operation that parses and validates 40+ config fields.
   *
   * The cache is populated lazily on first access via [[getTopicConfig]]. The built-in engine keeps
   * it up to date from the engine callbacks for topic configuration changes, topic deletion, and
   * broker log default changes.
   */
  private val topicConfigs = new ConcurrentHashMap[String, LogConfig]()

  private[metadata] def getDefaultConfig: util.Map[String, Object] = {
    // Filter out null values as they break LogConfig initialization using Properties.putAll
    defaultConfig.get().asScala.filter(_._2 != null).asJava
  }

  // Only method requiring specific KRaftMetadataCache functionality.
  // If we could refactor RetentionEnforcement to not require this, we could use the MetadataView interface directly.
  override def getBrokerCount: Integer = metadataCache.currentImage().cluster().brokers().size()

  override def getTopicConfig(topicName: String): LogConfig = topicConfigs.computeIfAbsent(topicName, t => {
    val props = metadataCache.topicConfig(t)
    if (props.isEmpty) new LogConfig(getDefaultConfig)
    else LogConfig.fromProps(getDefaultConfig, props)
  })

  override def updateTopicConfig(topicName: String, topicOverrides: Properties): Unit = {
    topicConfigs.computeIfPresent(topicName, (_, _) => LogConfig.fromProps(getDefaultConfig, topicOverrides))
  }

  override def removeTopicConfig(topicName: String): Unit = {
    topicConfigs.remove(topicName)
  }

  override def reconfigureDefaultLogConfig(): Unit = {
    val newDefaults = getDefaultConfig
    topicConfigs.replaceAll { (_, existingConfig) =>
      val props = new util.HashMap[String, Object](newDefaults)
      existingConfig.originals.asScala
        .filter { case (k, _) => existingConfig.overriddenConfigs.contains(k) }
        .foreach { case (k, v) => props.put(k, v) }
      new LogConfig(props, existingConfig.overriddenConfigs)
    }
  }
}
