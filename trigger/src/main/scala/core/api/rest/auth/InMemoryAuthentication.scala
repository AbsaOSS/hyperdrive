package core.api.rest.auth

import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.crypto.password.NoOpPasswordEncoder
import org.springframework.stereotype.Component

@Component
class InMemoryAuthentication extends HyperdriverAuthentication {
  val username: String = "hyperdriver-user"
  val password: String = "hyperdriver-password"

  def validateParams() {
    if (username.isEmpty || password.isEmpty) {
      throw new IllegalArgumentException("Both username and password have to configured for inmemory authentication.")
    }
  }

  override def configure(auth: AuthenticationManagerBuilder) {
    this.validateParams()
    auth
      .inMemoryAuthentication()
      .passwordEncoder(NoOpPasswordEncoder.getInstance())
      .withUser(username)
      .password(password)
      .authorities("ROLE_USER")
  }
}
