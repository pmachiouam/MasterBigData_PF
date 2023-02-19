package org.uam.masterbigdata

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, lag, lit, when}

object JourneysHelper {

    def calculateJourneys()(df:DataFrame):DataFrame = {
        /*Por cada paso deberíamos tener una función*/
        //crear función para detectar los cambio de llave (ver check_is_in_activity)
          //setIgnitionStateChange
        //crear columna que en caso de haber un cambio establecer el valor 1 y en caso de no haber cambio establece un 0
        //Sumar la columna anterior de forma que cada vez que hay un cambio se suma uno y todos las posiciones del mismo cambio tienen el mismo valor.
        //agrupamos por ese valor y nos quedamos con los que tienen la ignición uno, asi tenemos las posiciones de cada trayecto
        //agregamos las posiciones de cada trayecto, generamos el uid, calculamos las distancia y establecemos los valores de inicio y fin.
        
        ???
    }
    //Recuerda meter la validación del esquema
    def setIgnitionStateChange()(df:DataFrame):DataFrame = {
        val window = Window
          .partitionBy(col("attributes").getField("deviceId"))
          .orderBy(col("timestamp"))

        df.withColumn("state_changed"
            , when(col("ignition") === coalesce(lag(col("ignition"),1).over(window), col("ignition")), 0)
            .when(col("ignition") =!= coalesce(lag(col("ignition"),1).over(window), col("ignition")), 1)
        )
    }

}
