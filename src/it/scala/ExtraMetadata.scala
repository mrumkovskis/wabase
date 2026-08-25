package wabase.app

import org.mojoz.metadata.in.YamlMd
import org.wabase.AppMetadata

import scala.collection.immutable.Seq

/** Metadata source for the `app.wabase-extra-metadata.loader` config parameter.
  *
  * Definitions are assembled from strings at runtime instead of being read from `tables`, `views`
  * or `routes` resources, so that integration tests prove metadata arrives through the configured
  * loader and not through the ordinary resource path scan.
  *
  * Names given to definition strings end with `.yaml` to satisfy `app.public-api.views-location-pattern`,
  * this makes the extra view public, like views loaded from resources.
  *
  * Definitions from `app.wabase-extra-metadata.paths` are appended by delegating to
  * [[org.wabase.AppMetadata.metadataFromFiles]] - this covers the default loader implementation
  * and demonstrates how an application keeps file based extra metadata while contributing its own.
  */
object ExtraMetadata {
  private val tableDefs =
    """
      |table: extra_metadata_data
      |columns:
      |- code   !  20
      |- value     40
      |pk:
      |- code
      |""".stripMargin

  private val viewDefs =
    """
      |name:   extra_metadata_view
      |table:  extra_metadata_data
      |api:    count, create, get, list, upsert, delete
      |key:    code
      |fields:
      |- code
      |- value
      |order:
      |- code
      |""".stripMargin

  private val routeDefs =
    """
      |on: GET /extra-metadata-hello
      |do: response(200, 'Hello from extra metadata!')
      |swagger:
      |  '200':
      |    content:
      |      text/plain:
      |        type: string
      |""".stripMargin

  def load(): Seq[YamlMd] = YamlMd.fromNamedStrings(
    "extra-metadata-tables.yaml" -> tableDefs,
    "extra-metadata-views.yaml"  -> viewDefs,
    "extra-metadata-routes.yaml" -> routeDefs,
  ) ++ AppMetadata.metadataFromFiles()
}
