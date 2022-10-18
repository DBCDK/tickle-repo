#!groovy
@Library('metascrum')
import dk.dbc.metascrum.jenkins.MetascrumMaven

def workerNode = "devel11"

pipeline {
	agent {label workerNode}

	tools {
		maven 'Maven 3'
	}

    environment {
		GITLAB_PRIVATE_TOKEN = credentials("metascrum-gitlab-api-token")
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
				def m = new MetascrumMaven()
				m.verify(this)
			}
		}

		stage("deploy") {
			when {
                branch "master"
            }
			steps {
				sh "mvn -Dmaven.test.skip=true jar:jar deploy:deploy"
			}
		}
    }
}
