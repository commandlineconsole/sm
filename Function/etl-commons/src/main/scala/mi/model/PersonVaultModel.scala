package uk.gov..mi.model


case class LinkPerson(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                      lnk_person_idntty_hk: String, record_source: String, person_handle_id: String,
                      identity_handle_id: String, person_hk: String, identity_hk: String)

/**
  *
  * @param message_id
  * @param bsn_event_datetime
  * @param lnd_datetime
  * @param record_source
  * @param person_handle_id
  * @param rec_hash_value Hash  ($record_source + $person_handle_id + $identity_handle_id)
  * @param person_handle_visibility
  */
case class PersonVisibility(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                            record_source: String, person_handle_id: String, rec_hash_value: String,
                            person_handle_visibility: Option[String], person_hk: String)

/**
  *
  * @param message_id
  * @param bsn_event_datetime
  * @param lnd_datetime
  * @param record_source
  * @param person_handle_id
  * @param rec_hash_value Hash ($person_handle_visibility)
  * @param src_person_space
  * @param src_created_by
  * @param src_created_datetime
  */
case class PersonChangeLog(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                           record_source: String, person_handle_id: String, rec_hash_value: String,
                           src_person_space: Option[String], src_created_by: Option[String], src_created_datetime: Option[String],
                           person_hk: String)

/**
  *
  * @param message_id
  * @param bsn_event_datetime
  * @param lnd_datetime
  * @param person_hk Hash ($record_source + $person_handle_id)
  * @param record_source
  * @param person_handle_id
  */
case class HubPerson(message_id: String, bsn_event_datetime: Option[String],
                     lnd_datetime: String, person_hk: String, record_source: String,
                     person_handle_id: String)
