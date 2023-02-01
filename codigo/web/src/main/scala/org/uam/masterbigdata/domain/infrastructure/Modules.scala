package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.infrastructure.repository.{EventsRepository, JourneysRepository}

trait PersistenceModule {
  val journeysRepository : JourneysRepository
  val eventsRepository: EventsRepository
}

trait DataAccessLayer
  extends DbConfigModule
    with DbModule
    with PersistenceModule
