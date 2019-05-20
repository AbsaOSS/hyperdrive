package core.api.rest.controllers

import core.models.UserInfo
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._

@RestController 
class UserInfoController {

  @GetMapping(path = Array("/user/info"))
  def userInfo(): UserInfo = {
    val auth = SecurityContextHolder.getContext.getAuthentication
    val principal = auth.getPrincipal.asInstanceOf[UserDetails]
    UserInfo(principal.getUsername)
  }

}
