package core.api.rest.controllers

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint
import org.springframework.core.env.Environment
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.{GetMapping, RequestParam}
import org.springframework.core.io.ByteArrayResource
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

@Component
@RestControllerEndpoint(id = "logfiles")
class ManagementCustomEndpoint {

  @Autowired
  val env: Environment = null

  @GetMapping(path = Array("/"))
  def getLogArchives: ResponseEntity[Seq[String]] = {
    val logsDir = new File(env.getProperty("logging.path"))
    val result = if(logsDir.exists && logsDir.isDirectory) {
      logsDir.listFiles.filter {
        e => e.isFile && e.getName.endsWith(".gz")
      }.map(_.getName)
    } else {
      Array[String]()
    }
    new ResponseEntity(result, HttpStatus.OK)
  }

  @GetMapping(path = Array("/download"))
  def downloadLogArchive(@RequestParam fileName: String): ResponseEntity[ByteArrayResource] = {
    val logFile = new File(s"${env.getProperty("logging.path")}/$fileName")
    val header = new HttpHeaders()
    header.add(HttpHeaders.CONTENT_DISPOSITION, s"attachment; filename=$fileName")
    header.add("Cache-Control", "no-cache, no-store, must-revalidate")
    header.add("Pragma", "no-cache")
    header.add("Expires", "0")

    val logFilePath = Paths.get(logFile.getAbsolutePath)
    val resource = new ByteArrayResource(Files.readAllBytes(logFilePath))

    ResponseEntity.ok()
      .headers(header)
      .contentLength(logFile.length())
      .contentType(MediaType.parseMediaType("application/octet-stream"))
      .body(resource)
  }
}
