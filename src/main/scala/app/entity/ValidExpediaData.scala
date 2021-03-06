package app.entity

/** Validated booking data got from HDFS */
case class ValidExpediaData(id: Long,
                            date_time: String,
                            site_name: Int,
                            posa_continent: Int,
                            user_location_country: Int,
                            user_location_region: Int,
                            user_location_city: Int,
                            orig_destination_distance: Double,
                            user_id: Int,
                            is_mobile: Int,
                            is_package: Int,
                            channel: Int,
                            srch_ci: String,
                            srch_co: String,
                            srch_adults_cnt: Int,
                            srch_children_cnt: Int,
                            srch_rm_cnt: Int,
                            srch_destination_id: Int,
                            srch_destination_type_id: Int,
                            hotel_id: Double,
                            idle_days: Long) extends Serializable

