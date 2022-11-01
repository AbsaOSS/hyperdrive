
package za.co.absa.hyperdrive.ingestor.api.secrets

class SecretString(secret: String) {
  override def toString: String = "*****"
  def retrieveValue: String = secret
}
