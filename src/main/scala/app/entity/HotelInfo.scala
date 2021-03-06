package app.entity

/** Hotel data from Kafka */
case class HotelInfo(id: Double,
                     name: String,
                     country: String,
                     city: String,
                     address: String,
                     latitude: String,
                     longitude: String,
                     geohash: String) extends Serializable
