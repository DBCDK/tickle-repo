#!groovy
@Library('metascrum')
import dk.dbc.metascrum.jenkins.Maven
// Defined in https://github.com/DBCDK/metascrum-pipeline-library/blob/master/src/dk/dbc/metascrum/jenkins/Maven.groovy

def workerNode = "devel11"

pipeline {
	agent {label workerNode}

	tools {
		maven 'Maven 3'
	}

    environment {
		GITLAB_PRIVATE_TOKEN = credentials("metascrum-gitlab-api-token")
		MAVEN_OPTS="-Dmaven.repo.local=\$WORKSPACE/.repo"
	}

	options {
		timestamps()
		disableConcurrentBuilds()
	}

	stages {
		stage("clear workspace") {
			steps {
				deleteDir()
				checkout scm
			}
		}

		stage("verify") {
			steps {
				script {
 					Maven.verify(this)
				}
			}
		}

		stage("deploy") {
			when {
                branch "master"
            }
			steps {
				script {
					Maven.deploy(this)
				}
			}
		}
    }
}
