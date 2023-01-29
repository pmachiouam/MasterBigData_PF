package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.infrastructure.repository.JourneysRepository

trait PersistenceModule {
  val journeysRepository : JourneysRepository
}

trait DataAccessLayer
  extends DbConfigModule
    with DbModule
    with PersistenceModule
