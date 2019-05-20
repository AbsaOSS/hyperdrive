package core.api.rest

import core.api.rest.auth.{HyperdriverAuthentication, InMemoryAuthentication}
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpStatus
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter
import org.springframework.security.core.Authentication
import org.springframework.security.core.AuthenticationException
import org.springframework.security.web.AuthenticationEntryPoint
import org.springframework.security.web.authentication.AuthenticationFailureHandler
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler
import org.springframework.security.web.authentication.logout.HttpStatusReturningLogoutSuccessHandler
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler
import org.springframework.security.web.csrf.CsrfToken
import org.springframework.stereotype.Component
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import org.springframework.security.authentication.AuthenticationManager

@EnableWebSecurity
class WebSecurityConfig {

  @Autowired
  val beanFactory: BeanFactory = null

  @Bean
  def authenticationFailureHandler(): AuthenticationFailureHandler = {
    new SimpleUrlAuthenticationFailureHandler()
  }

  @Bean
  def logoutSuccessHandler(): LogoutSuccessHandler = {
    new HttpStatusReturningLogoutSuccessHandler(HttpStatus.OK)
  }

  @Configuration
  class ApiWebSecurityConfigurationAdapter @Autowired() (
    restAuthenticationEntryPoint: RestAuthenticationEntryPoint,
    authenticationSuccessHandler: AuthSuccessHandler,
    authenticationFailureHandler: AuthenticationFailureHandler,
    logoutSuccessHandler: LogoutSuccessHandler
  ) extends WebSecurityConfigurerAdapter {

    override def configure(http: HttpSecurity) {
      http
        .csrf()
        .ignoringAntMatchers("/login")
        .and()

        .exceptionHandling()
        .authenticationEntryPoint(restAuthenticationEntryPoint)
        .and()

        .authorizeRequests()
        .antMatchers(
          "/", "/index.html", "/css/**", "/webjars/**",
          "/repository/**", "/Component.js", "/manifest.json",
          "/i18n/**", "/model/**", "/view/**",
          "/controller/**", "/lib/**", "/generic/**",
          "/favicon.ico"
        )
        .permitAll()
        .anyRequest()
        .authenticated()
        .and()

        .formLogin()
        .loginProcessingUrl("/login")
        .successHandler(authenticationSuccessHandler)
        .failureHandler(authenticationFailureHandler)
        .permitAll()
        .and()

        .logout()
        .logoutUrl("/logout")
        .logoutSuccessHandler(logoutSuccessHandler)
        .permitAll()
        .clearAuthentication(true)
        .deleteCookies("JSESSIONID")
        .invalidateHttpSession(true)
    }


    override def configure(auth: AuthenticationManagerBuilder) {
      this.getHyperdriverAuthentication().configure(auth)
    }

    private def getHyperdriverAuthentication(): HyperdriverAuthentication = {
      beanFactory.getBean(classOf[InMemoryAuthentication])
    }

    @Bean
    override def authenticationManagerBean(): AuthenticationManager = {
      super.authenticationManagerBean()
    }
  }

  @Component
  class RestAuthenticationEntryPoint extends AuthenticationEntryPoint {
    override def commence(
      request: HttpServletRequest,
      response: HttpServletResponse,
      authException: AuthenticationException
    ): Unit = {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized")
    }
  }

  @Component
  class AuthSuccessHandler extends SimpleUrlAuthenticationSuccessHandler {

    override def onAuthenticationSuccess(
      request: HttpServletRequest,
      response: HttpServletResponse,
      authentication: Authentication
    ): Unit = {
      val csrfToken = request.getAttribute("_csrf").asInstanceOf[CsrfToken]
      response.addHeader(csrfToken.getHeaderName, csrfToken.getToken)
      clearAuthenticationAttributes(request)
    }

  }

}
