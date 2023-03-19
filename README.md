# Estructura

- vm-spark3.1/vmfiles/ -> Carpeta que se monta en la maquina virtual se usa para compartir archivos con la máquina virtual
- vm-spark3.1/vmfiles/apps -> Carpeta que se dejan las aplicaciones para ejecutar
- vm-spark3.1/vmfiles/data -> Carpeta contiene los datos brutos que se usarán

# Script

Dentro de la máquina virtual
No tenemos un cluster, por lo que no hay un Spark-Masgter escuchando en el puerto 7077 para hacer el deploy, por eso se omite. Los parametros finales son el jar a ejecutar con el código y los parámetros que le pasamos
spark-submit --class part6practical.TestDeployApp --deploy-mode client --verbose --supervise /vagrant/spark-essentials.jar /vagrant/movies.json /vagrant/out

# Ejecución

## Web

Url: http://localhost:8080/api/v1.0/docs/index.html?url=/api/v1.0/docs/docs.yaml
Inicialización:
'''
java -jar path_archivo/web.jar
ejemplo: java -jar codigo/web/target/scala-2.12/web.jar
'''
