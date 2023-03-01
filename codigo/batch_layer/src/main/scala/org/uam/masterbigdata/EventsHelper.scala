package org.uam.masterbigdata

import org.apache.spark.sql.DataFrame

object EventsHelper {
  /**Creates a event based on the abusive throttle use (more than 20%)*/
  def createExcessiveThrottleEvent()(df:DataFrame):DataFrame = {
    ???//df.transform()
  }

}
