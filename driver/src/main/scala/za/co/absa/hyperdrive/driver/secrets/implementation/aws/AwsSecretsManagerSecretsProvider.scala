
package za.co.absa.hyperdrive.driver.secrets.implementation.aws

import za.co.absa.hyperdrive.ingestor.api.secrets.{SecretString, SecretsProvider}

class AwsSecretsManagerSecretsProvider extends SecretsProvider {
  override def retrieveSecret(options: Map[String, String]): SecretString = ???
}
