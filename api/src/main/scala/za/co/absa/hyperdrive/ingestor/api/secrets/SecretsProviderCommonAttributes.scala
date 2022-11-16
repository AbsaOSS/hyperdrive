
package za.co.absa.hyperdrive.ingestor.api.secrets

object SecretsProviderCommonAttributes {
  val baseKey = "secretsprovider"
  val configKey = s"${baseKey}.config"
  val configProvidersKey = s"${configKey}.providers"
  val configDefaultProviderKey = s"${configKey}.defaultprovider"
  val secretsKey = s"${baseKey}.secrets"
  val perSecretProviderKey = "provider"
  val perSecretSecretValueKey = "secretvalue"
  val perSecretOptionsKey = "options"
  val perProviderClassKey = "class"
}
