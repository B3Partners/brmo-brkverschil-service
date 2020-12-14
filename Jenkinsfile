timestamps {

    properties([
        buildDiscarder(logRotator(artifactDaysToKeepStr: '8', artifactNumToKeepStr: '3', daysToKeepStr: '15', numToKeepStr: '5')),
        pipelineTriggers([
            [$class: 'PeriodicFolderTrigger', interval: '15m']
        ])
    ]);

    node {
        withEnv(["JAVA_HOME=${ tool 'OpenJDK8' }", "PATH+MAVEN=${tool 'Maven CURRENT'}/bin:${env.JAVA_HOME}/bin"]) {

            stage('Prepare') {
                sh "ulimit -a"
                sh "free -m"
                checkout scm
            }

            stage('Build') {
                echo "Building branch: ${env.BRANCH_NAME}"
                sh "mvn install -Dmaven.test.skip=true -B -V -e -fae -q"
            }

            stage('Lint') {
                echo "Running Lint checks"
                sh "mvn javadoc:javadoc"
                sh "mvn javadoc:test-javadoc"
            }

            stage('Test') {
                echo "Running unit tests"
                sh "mvn -e test -B"
            }
            lock('tomcat-tcp9091') {
                timeout(10) {
                    stage('Integration Test') {
                        echo "Running integration tests"
                        sh "mvn -e -B clean verify -Ppostgresql"
                    }
                }
            }

            stage('Publish Test Results') {
                junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml, **/target/failsafe-reports/TEST-*.xml'
            }

            stage('Test Coverage results') {
                jacoco exclusionPattern: '**/*Test*.class', classPattern: '**/target/classes', execPattern: '**/target/**.exec'
            }

            stage('OWASP Dependency Check') {
                echo "Uitvoeren OWASP dependency check"
                sh "mvn org.owasp:dependency-check-maven:check"
                dependencyCheckPublisher failedNewCritical: 1, failedNewHigh: 1, failedNewLow: 2, failedNewMedium: 2, failedTotalCritical: 1, failedTotalHigh: 1, failedTotalLow: 5, failedTotalMedium: 5, pattern: '**/dependency-check-report.xml'
            }
        }
    }
}
