/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

def hyperdriveSlaveLabel = getHyperdriveSlaveLabel()
def toolVersionJava = getToolVersionJava()
def toolVersionMaven = getToolVersionMaven()
def toolVersionGit = getToolVersionGit()
def mavenSettingsId = getMavenSettingsId()

pipeline {
    agent {
        label "${hyperdriveSlaveLabel}"
    }
    
    tools {
        jdk "${toolVersionJava}"
        maven "${toolVersionMaven}"
        git "${toolVersionGit}"
    }
    options {
        buildDiscarder(logRotator(numToKeepStr: '20'))
        timestamps()
    }
    stages {
        stage ('Build') {
            steps {
                configFileProvider([configFile(fileId: "${mavenSettingsId}", variable: 'MAVEN_SETTINGS_XML')]) {
                    sh "mvn -s $MAVEN_SETTINGS_XML clean verify --no-transfer-progress"
                }
            }
        }
    }
}
