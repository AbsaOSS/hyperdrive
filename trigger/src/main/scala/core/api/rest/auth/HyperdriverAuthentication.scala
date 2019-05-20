package core.api.rest.auth

import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder

trait HyperdriverAuthentication {
  def configure(auth: AuthenticationManagerBuilder)
}
