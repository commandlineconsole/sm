package uk.gov.ipt.mi.model


case class HubServiceDelivery(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                              srvc_dlvry_hk: String, record_source: String, srvc_dlvry_handle_id: String)

case class HubDocAttachement(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                             doc_atchmnt_hk: String, record_source: String, srvc_dlvry_handle_id: String,
                             invlmnt_handle_id: String, doc_atchmnt_handle_id: String)

case class HubCorrespondence(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                             corr_hk: String, record_source: String, srvc_dlvry_handle_id: String,
                             invlmnt_handle_id: String, doc_atchmnt_handle_id: String, corr_handle_id: String)

case class SServiceDeliveryInfo(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                record_source: String, srvc_dlvry_handle_id: String, chlg_rec_hash_value: String,
                                src_created_by: Option[String], src_created_datetime: Option[String], src_last_updated_by: Option[String],
                                src_last_updated_datetime: Option[String], src_created2_datetime: Option[String], vis_rec_hash_value: String,
                                srvc_dlvry_handle_visibility: Option[String], eh_rec_hash_value: String, external_handle_value: Option[String],
                                external_handle_space: Option[String], main_rec_hash_value: String, srvc_dlvry_type_cd: String,
                                due_date: Option[String], priority: Option[Int], primary_ref_type_cd: Option[String],
                                primary_ref_value: Option[String], status_rec_hash_value: String, srvc_dlvry_status_cd: String,
                                srvc_dlvry_hk: String)

case class LinkServiceDeliveryParent(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                     lnk_srvc_dlvry_parent_hk: String, record_source: String, srvc_dlvry_handle_id: String,
                                     parent_srvc_dlvry_handle_id: Option[String], srvc_dlvry_hk: String, parent_srvc_dlvry_hk: String)


case class SServiceDeliveryAttribute(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                     record_source: String, srvc_dlvry_handle_id: String, attr_handle_id: String,
                                     rec_seqno: Int, rec_hash_value: String, attr_handle_visibility: Option[String],
                                     attr_type_cd: Option[String], attr_value_datetime: Option[String], attr_value_string: Option[String],
                                     attr_value_number: Option[Int], attr_ref_data_cd: Option[String], attr_created_by: Option[String],
                                     attr_created_datetime: Option[String], attr_list_agg_hash: String, lnk_srvcdlvry_attr_list_hk: String,
                                     srvc_dlvry_hk: String)


case class SServiceDeliveryProcess(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                   record_source: String, srvc_dlvry_handle_id: String, process_handle_id: String,
                                   rec_seqno: Int, rec_hash_value: String, process_handle_visibility: Option[String],
                                   srvc_dlvry_stage_cd: String, process_status_cd: String, process_created_by: Option[String],
                                   process_created_datetime: Option[String], process_last_updated_by: Option[String], process_last_updated_datetime: Option[String],
                                   process_created2_datetime: Option[String], process_list_agg_hash: String, lnk_srvcdlvry_prc_list_hk: String,
                                   srvc_dlvry_hk: String)

case class SServiceDeliveryProcessInstance(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                           record_source: String, srvc_dlvry_handle_id: String, process_inst_id: BigInt,
                                           rec_seqno: Int, rec_hash_value: String, process_id: BigInt,
                                           process_inst_status_cd: String, process_inst_stage_cd: String, process_inst_start_datetime: Option[String],
                                           process_inst_end_datetime: Option[String], process_inst_list_agg_hash: String, lnk_srvcdlvry_prc_inst_list_hk: String,
                                           srvc_dlvry_hk: String)

case class SServiceDeliveryDocAttachmentInfo(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                             record_source: String, srvc_dlvry_handle_id: String, invlmnt_handle_id: String,
                                             doc_atchmnt_handle_id: String, main_rec_hash_value: String, doc_atchmnt_handle_visibility: Option[String],
                                             doc_atchmnt_type_cd: String, doc_atchmnt_external_ref: String, doc_atchmnt_desc: Option[String],
                                             doc_atchmnt_doc_store_id: String, doc_atchmnt_mime_type_cd: String, doc_atchmnt_record_datetime: Option[String],
                                             doc_atchmnt_provider_cd: String, doc_atchmnt_verified_flag: Boolean, doc_atchmnt_stage_cd: String,
                                             chlg_rec_hash_value: String, doc_atchmnt_created_by: Option[String], doc_atchmnt_created_datetime: Option[String],
                                             doc_atchmnt_last_updated_by: Option[String], doc_atchmnt_last_updated_dtime: Option[String],
                                             srvc_dlvry_hk: String, invlmnt_hk: String, doc_atchmnt_hk: String)

case class SServiceDeliveryCorrInfo(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                    record_source: String, srvc_dlvry_handle_id: String, invlmnt_handle_id: String,
                                    doc_atchmnt_handle_id: String, corr_handle_id: String, corr_rec_hash_value: String,
                                    corr_handle_visibility: Option[String], corr_delivery_note: Option[String], corr_created_by: Option[String],
                                    corr_created_datetime: Option[String], corr_last_updated_by: Option[String],
                                    corr_last_updated_datetime: Option[String], corr_status_cd: Option[String], po_addr_handle_id: Option[String],
                                    el_addr_handle_id: Option[String], lnk_corr_addr_hk: String, srvc_dlvry_hk: String,
                                    invlmnt_hk: String, doc_atchmnt_hk: String, corr_hk: String)


case class HubServiceDeliveryInvolvement(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                         invlmnt_hk: String, record_source: String, srvc_dlvry_handle_id: String,
                                         invlmnt_handle_id: String, invlmnt_method_cd: String)

case class SServiceDeliveryInvolvementInfo(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                           record_source: String, srvc_dlvry_handle_id: String, invlmnt_handle_id: String,
                                          invlmnt_method_cd: String, chlg_rec_hash_value: String, src_created_by: Option[String],
                                          src_created_datetime: Option[String], src_last_updated_by: Option[String], src_last_updated_datetime: Option[String],
                                          src_created2_datetime: Option[String], vis_rec_hash_value: String, invlmnt_handle_visibility: Option[String],
                                          invlmnt_hk: String)

case class HubServiceDeliveryInvolvedPerson(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                            involved_person_hk: String, record_source: String, srvc_dlvry_handle_id: String,
                                            involved_person_id: Option[String])

case class SServiceDeliveryInvolvedPersonInfo(message_id: String, bsn_event_datetime: Option[String],
                                              lnd_datetime: String, record_source: String, srvc_dlvry_handle_id: String,
                                              involved_person_id: Option[String], chlg_rec_hash_value: String, involved_person_created_by: Option[String],
                                              involved_person_created_dtime: Option[String] , default_biog_rec_hash_value: String, title_cd: String,
                                              lang_cd: String, given_name: String, family_name: String,
                                              full_name: String, gender_cd: String, day_of_birth: String,
                                              month_of_birth: String, year_of_birth: String, date_of_birth: String,
                                              photo_url: String, nationality_cd: String, place_of_birth: String,
                                              passport: String, involved_person_hk: String
                                               )

case class SServiceDeliveryInvolvedPersonRecentBiographics(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                                           record_source: String, srvc_dlvry_handle_id: String, involved_person_id: Option[String],
                                                           biographic_handle_id: Option[String], rec_seqno: Int, rec_hash_value: String,
                                                           biographic_handle_visibility: Option[String], biographic_type_cd: String, biographic_value: String,
                                                           recent_biog_list_agg_hash: String, involved_person_hk: String
                                                            )

case class LinkServiceDeliveryInvolvementParty(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                              lnk_srvcdlvry_party_invlmnt_hk: String, record_source: String, srvc_dlvry_handle_id: String,
                                              invlmnt_handle_id: String, invlmnt_method_cd: String, person_handle_id: Option[String],
                                              involved_person_id: Option[String], identity_handle_id: Option[String], org_handle_id: Option[String],
                                              individual_handle_id: Option[String], invlmnt_role_type_cd: String, invlmnt_role_subtype_cd: Option[String],
                                              srvc_dlvry_hk: String, invlmnt_hk: String, person_hk: String,
                                              involved_person_hk: String, identity_hk: String, org_hk: String,
                                              individual_hk: String)

case class SServiceDeliveryInvolvementPostalAddress(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                                    record_source: String, srvc_dlvry_handle_id: String, invlmnt_handle_id: String,
                                                    invlmnt_method_cd: String, invlmnt_po_addr_handle_id: String, po_addr_handle_id: Option[String],
                                                    invlmnt_po_addr_usage_type_cd: Option[String], rec_seqno: Int, rec_hash_value: String,
                                                    invlmnt_po_addr_visibility: Option[String], invlmnt_po_addr_start_dtime: Option[String], invlmnt_po_addr_end_dtime: Option[String],
                                                    invlmnt_po_addr_created_by: Option[String], invlmnt_po_addr_created_dtime: Option[String], po_addr_hk: String,
                                                    lnk_invlmnt_po_addr_hk: String, invlmnt_po_addr_list_agg_hash: String, invlmnt_hk: String)

case class SServiceDeliveryInvolvementElectronicAddress(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                                        record_source: String, srvc_dlvry_handle_id: String, invlmnt_handle_id: String,
                                                        invlmnt_method_cd: String, invlmnt_el_addr_handle_id: String, invlmnt_el_addr_type_cd: Option[String],
                                                        rec_seqno: Int, rec_hash_value: String, invlmnt_el_addr_visibility: Option[String],
                                                        invlmnt_el_addr_value: Option[String], invlmnt_el_addr_start_dtime: Option[String], invlmnt_el_addr_end_dtime: Option[String],
                                                        invlmnt_el_addr_created_by: Option[String], invlmnt_el_addr_created_dtime: Option[String],
                                                        el_addr_hk: String, lnk_invlmnt_el_addr_hk: String, invlmnt_el_addr_list_agg_hash: String,
                                                        invlmnt_hk: String)

case class SServiceDeliveryInvolvementNote(message_id: String, bsn_event_datetime: Option[String], lnd_datetime: String,
                                           record_source: String, srvc_dlvry_handle_id: String, invlmnt_handle_id: String,
                                           rec_hash_value: String, note_id: String, note_type_cd: String, note_text: String,
                                           invlmnt_hk: String )
