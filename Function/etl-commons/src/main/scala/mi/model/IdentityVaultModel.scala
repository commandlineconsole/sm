package uk.gov..mi.model

case class HubIdentity(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                       identity_hk: String, record_source: String, identity_handle_id: String)

case class SIdentityInfo(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                         record_source: String, identity_handle_id: String, chlg_rec_hash_value: String,
                         src_cd: Option[String], src_created_by: Option[String], src_created_datetime: Option[String],
                         aud_created_by: Option[String], aud_created_datetime: Option[String], aud_last_updated_by: Option[String],
                         aud_last_updated_datetime: Option[String], vis_rec_hash_value: String, identity_handle_visibility: Option[String],
                         eh_rec_hash_value: String, eh_external_handle_value: Option[String], eh_external_handle_space: Option[String],
                         identity_hk: String)

case class SIdentityBiometricInfo(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                  record_source: String, identity_handle_id: String, biometric_handle_id: String,
                                  rec_seqno: Int, rec_hash_value: String, biometric_handle_visibility: Option[String],
                                  biometric_ext_handle_value: Option[String], biometric_ext_handle_space: Option[String], biometric_nature: Option[String],
                                  biometric_type_cd: Option[String], biometric_value: Option[String], biometric_created_by: Option[String],
                                  biometric_created_datetime: Option[String], biometric_list_agg_hash: String, lnk_idntty_biometric_list_hk: String,
                                  identity_hk: String)

case class SIdentityReference(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                              record_source: String, identity_handle_id: String, reference_handle_id: String,
                              rec_seqno: Int, rec_hash_value: String, reference_visibility: Option[String],
                              reference_ext_handle_value: Option[String], reference_ext_handle_space: Option[String], reference_type_cd: Option[String],
                              reference_value: Option[String], reference_created_by: Option[String], reference_created_datetime: Option[String],
                              reference_list_agg_hash: String, lnk_idntty_reference_list_hk: String, identity_hk: String)

case class SIdentityCondition(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                              record_source: String, identity_handle_id: String, condition_handle_id: String,
                              rec_seqno: Int, rec_hash_value: String, condition_handle_visibility: Option[String],
                              condition_ext_handle_value: Option[String], condition_ext_handle_space: Option[String], condition_start_datetime: Option[String],
                              condition_end_datetime: Option[String], condition_type_cd: Option[String], condition_update_datetime: Option[String],
                              condition_note: Option[String], condition_created_by: Option[String], condition_created_datetime: Option[String],
                              condition_originated_by: Option[String], condition_list_agg_hash: String, lnk_idntty_condition_list_hk: String,
                              identity_hk: String)

case class SIdentityBiographics(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                record_source: String, identity_handle_id: String, biog_superset_agg_hash: String,
                                biog_set_handle_id: String, biog_set_rec_hash_value: String, biog_set_handle_visibility: Option[String],
                                biog_set_ext_handle_value: Option[String], biog_set_ext_handle_space: Option[String], biog_set_purpose_cd: Option[String],
                                biog_set_created_by: Option[String], biog_set_created_datetime: Option[String], biog_set_agg_hash: String,
                                lnk_idntty_biog_set_hk: String, biographic_key: Option[String], biographic_handle_id: Option[String], biographic_rec_seqno: Option[Int],
                                biographic_rec_hash_value: Option[String], biographic_handle_visibility: Option[String], biographic_type_cd: Option[String],
                                biographic_value: Option[String], biographic_value_type_cd: Option[String], biographic_created_by: Option[String],
                                biographic_created_datetime: Option[String], biographic_reference_data_set: Option[String], identity_hk: String)


case class SIdentityDescrors(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                record_source: String, identity_handle_id: String, descr_superset_agg_hash: String,
                                descr_set_handle_id: String, descr_set_rec_hash_value: String, descr_set_handle_visibility: Option[String],
                                descr_set_ext_handle_value: Option[String], descr_set_ext_handle_space: Option[String], descr_set_created_by: Option[String],
                                descr_set_created_datetime: Option[String], descr_set_agg_hash: String, lnk_idntty_descr_set_hk: String,
                                descr_handle_id: Option[String], descr_rec_seqno: Option[Int], descr_rec_hash_value: Option[String],
                                descr_handle_visibility: Option[String], descr_type_cd: Option[String], descr_value: Option[String],
                                descr_created_by: Option[String], descr_created_datetime: Option[String], identity_hk: String)

case class SIdentityMedia(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                          record_source: String, identity_handle_id: String, media_superset_agg_hash: String,
                          media_set_handle_id: String, media_set_rec_hash_value: String, media_set_handle_visibility: Option[String],
                          media_set_ext_handle_value: Option[String], media_set_ext_handle_space: Option[String], media_set_created_by: Option[String],
                          media_set_created_datetime: Option[String], media_set_agg_hash: String, lnk_idntty_media_set_hk: String,
                          media_handle_id: Option[String], media_rec_seqno: Option[Int], media_rec_hash_value: Option[String],
                          media_handle_visibility: Option[String], media_type_cd: Option[String], media_file_handle_id: Option[String],
                          media_created_by: Option[String], media_created_datetime: Option[String], identity_hk: String)

case class LinkIdentityPerson(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                              lnk_idntty_person_hk: String, record_source: String, identity_handle_id: String,
                              person_handle_id: String, person_hk: String, identity_hk: String)
