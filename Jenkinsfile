#!groovy

@Library('dependency-track')

def workerNode = "devel12"

pipeline {
	agent {label workerNode}
	options {
		timestamps()
	}
	stages {
		stage("clear workspace") {
			steps {
				deleteDir()
				checkout scm
			}
		}
		stage("build") {
			steps {
				withSonarQubeEnv(installationName: 'sonarqube.dbc.dk') {
					script {
						def status = sh returnStatus: true, script: """
                        mvn -B -Dmaven.repo.local=$WORKSPACE/.repo --no-transfer-progress verify
                        """

						def sonarOptions = "-Dsonar.branch.name=$BRANCH_NAME"
						if (env.BRANCH_NAME != 'master') {
							sonarOptions += " -Dsonar.newCode.referenceBranch=master"
						}
						status += sh returnStatus: true, script: """
                        mvn -B -Dmaven.repo.local=$WORKSPACE/.repo --no-transfer-progress $sonarOptions sonar:sonar
                        """

						junit testResults: '**/target/*-reports/*.xml'

						if (status != 0) {
							error("build failed")
						}
					}
				}
			}
		}
		stage("quality gate") {
			steps {
				// wait for analysis results
				timeout(time: 1, unit: 'HOURS') {
					waitForQualityGate abortPipeline: true
				}
			}
		}
		stage("supply-chain gate") {
			steps {
				script {
					dependencyTrackGate(
						projectBom:  'target/sbom-java.json',
						projectTeam: 'de-team',
						projectType: 'java',
					)
				}
			}
		}
		stage("publish") {
			when {
				branch "master"
			}
			steps {
				sh "mvn deploy -DskipTests -DskipITs"
			}
		}
	}
}

