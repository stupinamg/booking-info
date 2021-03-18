pipeline {
    agent any

    stages {
        stage('Hello') {
            steps {
                echo 'Hello World'
            }
        }
		stage('Build') {
			steps {
				echo "Compiling..."
				sh "${tool name: 'sbt 0.13.9', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt compile"
			}
		}
		stage('Unit Test') {
			steps {
				echo "Testing..."
				sh "${tool name: 'sbt 0.13.9', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt test"
			}
		}
    }
}